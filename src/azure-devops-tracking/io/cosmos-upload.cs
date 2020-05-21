////////////////////////////////////////////////////////////////////////////////
//
// Module: background-cosmos-upload.cs
//
// Notes:
//
// Uploading to cosmos is a slow process that can block continueing to download
// other data. Therefore we will do all of this work in a seperate thread. The
// data will be pushed through a series of queues.
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;

using models;
using ev27;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace ev27 {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class CosmosUpload<T> where T : IDocument
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public CosmosUpload(string prefixMessage, 
                                  Container helixContainer, 
                                  Queue<T> uploadQueue, 
                                  Func<T, string> getPartitionKey, 
                                  Action<T> trimDoc)
    {
        PrefixMessage = prefixMessage;
        
        // There is a ~2mb limit for size and there can be roughly 200 active
        // tasks at one time.
        CapSize = (long)2000000;

        DocumentSize = 0;
        SuccessfulDocumentCount = 0;
        FailedDocumentCount = 0;

        HelixContainer = helixContainer;

        GetPartitionKey = getPartitionKey;
        TrimDoc = trimDoc;

        Documents = new List<T>();
        UploadQueue = uploadQueue;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public async Task Finish()
    {
        int docCount = UploadQueue.Count;
        Console.WriteLine($"Uploading: {docCount} documents");
        await Upload();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    private string PrefixMessage { get; set; }

    public long CapSize { get; set; }
    public int DocCap = 90;
    private long DocumentSize = 0;
    private int SuccessfulDocumentCount = 0;
    private int FailedDocumentCount = 0;
    private Container HelixContainer { get; set; }

    private Func<T, string> GetPartitionKey { get; set; }
    private Action<T> TrimDoc { get; set; }
    private List<T> Documents { get; set; }
    private Queue<T> UploadQueue { get; set; }
    private Thread UploadThread { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Upload
    ////////////////////////////////////////////////////////////////////////////

    private async Task AddOperation(T document)
    {
        int docToInsertSize = document.ToString().Length;

        if (docToInsertSize > CapSize)
        {
            TrimDoc(document);
            docToInsertSize = document.ToString().Length;
        }

        if (DocumentSize + docToInsertSize >= CapSize || Documents.Count > DocCap)
        {
            Trace.Assert(DocumentSize < CapSize);
            await DrainCosmosOperations();
        }

        DocumentSize += docToInsertSize;
        Documents.Add(document);
    }

    private async Task DrainCosmosOperations()
    {
        List<Task<OperationResponse<T>>> cosmosOperations = new List<Task<OperationResponse<T>>>();
        foreach (var document in Documents)
        {
            cosmosOperations.Add(HelixContainer.CreateItemAsync<T>(document, new PartitionKey(GetPartitionKey(document))).CaptureOperationResponse(document));
        }

        Console.WriteLine($"[{UploadQueue.Count}] -- Remaining.");
 
        bool encounteredError = false;

        do
        {
            BulkOperationResponse<T> helixBulkOperationResponse = null;
            try
            {
                helixBulkOperationResponse = await Shared.ExecuteTasksAsync(cosmosOperations);
                Console.WriteLine($"{PrefixMessage}: Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
                Console.WriteLine($"{PrefixMessage}: Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                Console.WriteLine($"{PrefixMessage}: Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
                Console.WriteLine($"{PrefixMessage}: Failed {helixBulkOperationResponse.Failures.Count} documents");
            }
            catch (Exception e)
            {
                // This is generally a timeout of some sort. We will wait and retry

                encounteredError = true;
                Thread.Sleep(5 * 1000);
            }

            if (!encounteredError)
            {
                DocumentSize = 0;
                Documents.Clear();

                if (helixBulkOperationResponse.Failures.Count > 0)
                {
                    Console.WriteLine($"{PrefixMessage}: First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

                    foreach (var operationFailure in helixBulkOperationResponse.Failures)
                    {
                        CosmosException cosmosException = (CosmosException)operationFailure.Item2;

                        if (cosmosException.StatusCode != HttpStatusCode.Conflict)
                        {
                            // Ignore conflicts
                            Documents.Add(operationFailure.Item1);
                        }
                    }

                    Thread.Sleep(10 * 1000);
                }
                SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
                FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
            }
            
        } while (encounteredError);
        
    }

    private async Task Upload()
    {
        int beginDocumentsUploaded = SuccessfulDocumentCount;
        // This is the only consumer. We do not need to lock.
        while (UploadQueue.Count != 0)
        {
            T model = UploadQueue.Dequeue();

            if (model == null)
            {
                Console.WriteLine("Found null document.");
                continue;
            }

           await AddOperation(model);
        }

        await DrainCosmosOperations();

        int totalDocumentsUploaded = SuccessfulDocumentCount - beginDocumentsUploaded;
        Console.WriteLine($"Uploaded {totalDocumentsUploaded}.");
    }

}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace(ev27)

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
