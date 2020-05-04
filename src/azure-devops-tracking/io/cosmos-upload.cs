////////////////////////////////////////////////////////////////////////////////
//
// Module: cosmos-upload.cs
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

public interface IDocument
{
    public string Id { get; set; }
    public string Console { get; set; }
    public string Name { get; set; }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class CosmosUpload<T> where T : IDocument
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public CosmosUpload(string prefixMessage, object uploadLock, Container helixContainer, TreeQueue<T> uploadQueue, Func<T, string> getPartitionKey, Action<T> trimDoc)
    {
        if (!RunningUpload)
        {
            lock(uploadLock)
            {
                // Someone else could have beaten us in the lock
                // if so do nothing.
                if (!RunningUpload)
                {
                    UploadLock = uploadLock;

                    GetPartitionKey = getPartitionKey;
                    TrimDoc = trimDoc;
                    PrefixMessage = prefixMessage;
                    HelixContainer = helixContainer;
                    UploadQueue = uploadQueue;
                    RunningUpload = true;

                    // There is a ~2mb limit for size and there can be roughly 200 active
                    // tasks at one time.
                    CapSize = (long)2000000;
                    Documents.Clear();

                    UploadThread = new Thread (() => Upload(UploadQueue));
                    UploadThread.Start();
                }
            }
        }

        Debug.Assert(UploadQueue != null);
        Debug.Assert(UploadLock != null);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public void Finish(bool join = true)
    {
        UploadQueue.SignalToFinish();

        if (join)
        {
            UploadThread.Join();
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Static variables
    ////////////////////////////////////////////////////////////////////////////

    private static string PrefixMessage { get; set; }

    public static long CapSize { get; set; }
    private static long DocumentSize = 0;
    private static long RetrySize = 0;
    private static int SuccessfulDocumentCount = 0;
    private static int FailedDocumentCount = 0;
    private static Container HelixContainer { get; set; }

    private static Func<T, string> GetPartitionKey { get; set; }
    private static Action<T> TrimDoc { get; set; }

    private static List<T> Documents = new List<T>();
    private static int DocumentIndex = 0;

    private static bool RunningUpload = false;
    private static TreeQueue<T> UploadQueue { get; set; }
    private static Thread UploadThread { get; set; }
    private static object UploadLock { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Upload
    ////////////////////////////////////////////////////////////////////////////

    private static async Task AddOperation(T document)
    {
        int docToInsertSize = document.ToString().Length;

        if (docToInsertSize > CapSize)
        {
            TrimDoc(document);
            docToInsertSize = document.ToString().Length;
        }

        if (DocumentSize + docToInsertSize >= CapSize || (Documents.Count - DocumentsIndex) > 90)
        {
            Debug.Assert(DocumentSize < CapSize);
            await DrainCosmosOperations();
        }

        DocumentSize += docToInsertSize;
        Documents.Add(document);
    }

    private static async Task DrainCosmosOperations()
    {
        List<Task<OperationResponse<T>>> cosmosOperations = new List<Task<OperationResponse<T>>>();
        for (var do in Documents)
        {
            cosmosOperations.Add(HelixContainer.CreateItemAsync<T>(document, new PartitionKey(GetPartitionKey(document))).CaptureOperationResponse(document));
        }


        BulkOperationResponse<T> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(cosmosOperations);
        Console.WriteLine($"{PrefixMessage}: Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"{PrefixMessage}: Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"{PrefixMessage}: Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"{PrefixMessage}: Failed {helixBulkOperationResponse.Failures.Count} documents");

        DocumentIndex = Documents.Count;
        DocumentSize = 0;

        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"{PrefixMessage}: First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            lock(UploadLock)
            {
                foreach (var operationFailure in helixBulkOperationResponse.Failures)
                {
                    CosmosException cosmosException = (CosmosException)operationFailure.Item2;

                    if (cosmosException.StatusCode != HttpStatusCode.Conflict)
                    {
                        // Ignore conflicts
                        UploadQueue.Enqueue(operationFailure.Item1);
                    }
                }
            }

            Thread.Sleep(10 * 1000);
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

    private static void Upload(TreeQueue<T> queue)
    {
        // This is the only consumer. We do not need to lock.

        bool finished = false;
        while (!finished)
        {
            T model = queue.Dequeue(() => {
                if (queue.Finished)
                {
                    finished = true;
                }
                
                Thread.Sleep(5000);
            });

            if (model == null)
            {
                DrainCosmosOperations().Wait();
                Debug.Assert(finished);
                break;
            }

            AddOperation(model).Wait();
        }

        lock (UploadLock)
        {
            GetPartitionKey = null;

            CosmosOperations = null;
            CosmosRetryOperations = null;

            RunningUpload = false;
        }
    }

}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace(ev27)

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////