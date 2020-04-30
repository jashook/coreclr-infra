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

    public CosmosUpload(object uploadLock, Container helixContainer, TreeQueue<T> uploadQueue)
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
                
                    CosmosOperations = new List<Task<OperationResponse<T>>>();
                    CosmosRetryOperations = new List<Task<OperationResponse<T>>>();

                    FailedDocumentCount = 0;
                    SuccessfulDocumentCount = 0;

                    DocumentSize = 0;
                    RetrySize = 0;

                    HelixContainer = helixContainer;
                    UploadQueue = uploadQueue;

                    RunningUpload = true;

                    // There is a ~2mb limit for size and there can be roughly 200 active
                    // tasks at one time.
                    CapSize = (long)((1 * 1000 * 1000) * 1.5);


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

    public async Task Finish()
    {
        await UploadQueue.Finish();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Static variables
    ////////////////////////////////////////////////////////////////////////////

    private static long CapSize { get; set; }
    private static long DocumentSize { get; set; }
    private static long RetrySize { get; set; }
    private static int SuccessfulDocumentCount { get; set; }
    private static int FailedDocumentCount { get; set; }
    private static Container HelixContainer { get; set; }

    private static List<Task<OperationResponse<T>>> CosmosOperations { get; set; }
    private static List<Task<OperationResponse<T>>> CosmosRetryOperations { get; set; }

    private static bool RunningUpload { get; set; }
    private static TreeQueue<T> UploadQueue { get; set; }
    private static Thread UploadThread { get; set; }
    private static object UploadLock { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Upload
    ////////////////////////////////////////////////////////////////////////////

    private static async Task AddOperation(T document)
    {
        if (document.Console != null)
        {
            if (document.Console.Length > 1000*1000)
            {
                document.Console = document.Console.Substring(0, 1000*1000);
            }

            DocumentSize += document.Console.Length;
        }
        
        CosmosOperations.Add(HelixContainer.CreateItemAsync<T>(document, new PartitionKey(document.Name)).CaptureOperationResponse(document));
        
        if (DocumentSize > CapSize || CosmosOperations.Count > 50)
        {
            await DrainCosmosOperations();
            DocumentSize = 0;
            CosmosOperations = new List<Task<OperationResponse<T>>>();
        }
    }

    private static async Task AddRetryOperation(T document)
    {
        if (document.Console != null)
        {
            RetrySize += document.Console.Length;
        }
        CosmosRetryOperations.Add(HelixContainer.CreateItemAsync<T>(document, new PartitionKey(document.Name)).CaptureOperationResponse(document));

        if (RetrySize > CapSize || CosmosRetryOperations.Count > 50)
        {
            await DrainRetryOperations();
            RetrySize = 0;
            CosmosRetryOperations = new List<Task<OperationResponse<T>>>();
        }
    }

    private static async Task DrainCosmosOperations()
    {
        List<Task<OperationResponse<T>>> copy = new List<Task<OperationResponse<T>>>();

        try
        {
            foreach (var item in CosmosOperations)
            {
                copy.Add(item);
            }
        }
        catch (Exception e)
        {
            foreach (var item in CosmosOperations)
            {
                copy.Add(item);
            }
        }

        CosmosOperations = new List<Task<OperationResponse<T>>>();
        BulkOperationResponse<T> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(copy);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await AddRetryOperation(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

    private static async Task DrainRetryOperations()
    {
        List<Task<OperationResponse<T>>> copy = new List<Task<OperationResponse<T>>>();

        foreach (var item in CosmosRetryOperations)
        {
            copy.Add(item);
        }

        CosmosRetryOperations = new List<Task<OperationResponse<T>>>();

        BulkOperationResponse<T> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(copy);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await AddRetryOperation(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

    private static void Upload(TreeQueue<T> queue)
    {
        // This is the only consumer. We do not need to lock.

        while (true)
        {
            T model = queue.Dequeue(() => {
                Thread.Sleep(5000);
            });

            AddOperation(model).Wait();
        }
    }

}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace(ev27)

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////