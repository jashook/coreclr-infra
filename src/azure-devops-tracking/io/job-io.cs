////////////////////////////////////////////////////////////////////////////////
//
// Module: job-io.cs
//
// Notes:
//
// Used to download as quickly and ingest data into cosmos as quickly as
// possible.
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

using Microsoft.Extensions.Configuration;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using models;
using DevOps.Util;

////////////////////////////////////////////////////////////////////////////////

public class JobIO
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public JobIO(Database db, object globalLock)
    {
        HelixContainer = db.GetContainer("helix-jobs");
        Container = db.GetContainer("runtime-jobs");
        CosmosOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        CosmosRetryOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();

        FailedDocumentCount = 0;
        SuccessfulDocumentCount = 0;

        DocumentSize = 0;
        RetrySize = 0;

        DownloadedJobs = 0;

        ActiveTasks = 0;

        Tasks = new List<Task>();

        // There is a ~2mb limit for size and there can be roughly 200 active
        // tasks at one time.
        CapSize = (long)((1 * 1000 * 1000) * .75);

        GlobalLock = globalLock;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    public int ActiveTasks { get; set; }
    public long CapSize { get; set; }
    public long DownloadedJobs { get; set; }
    public List<Task<OperationResponse<AzureDevOpsJobModel>>> CosmosOperations { get; set; }
    public List<Task<OperationResponse<AzureDevOpsJobModel>>> CosmosRetryOperations { get; set; }
    public long DocumentSize { get; set; }
    public int FailedDocumentCount { get; set; }
    public Container Container { get; set; }
    public Container HelixContainer { get; set; }
    public List<string> Jobs { get; set; }
    public long RetrySize { get; set; }
    public int SuccessfulDocumentCount { get; set; }
    public List<Task> Tasks { get; set; }
    public object GlobalLock  { get; set; }

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public async Task ReUploadData(FeedIterator<AzureDevOpsJobModel> iterator, bool force)
    {
        int procCount = Environment.ProcessorCount;
        while (iterator.HasMoreResults)
        {
            var cosmosResult = await iterator.ReadNextAsync();
            IEnumerable<AzureDevOpsJobModel> items = cosmosResult.Take(cosmosResult.Resource.Count());

            foreach (AzureDevOpsJobModel document in items)
            {
                Tasks.Add(ReplaceDocument(document, force));

                if (Tasks.Count > 5)
                {
                    await Task.WhenAll(Tasks);
                }

                int i =0;
            }

            await Task.WhenAll(Tasks);
        }

        await DrainCosmosOperations();
        await DrainRetryOperations();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////

    private async Task ReplaceDocument(AzureDevOpsJobModel document, bool force)
    {
        bool updated = false;
        foreach (AzureDevOpsStepModel step in document.Steps)
        {
            if (step.ConsoleUri != null &&
                (step.Name == "Initialize containers" || step.Result == TaskResult.Failed))
            {
                bool reDownloadConsole = true;
                if (step.Console != null && force == false)
                {
                    reDownloadConsole = false;
                }

                if (reDownloadConsole)
                {
                    step.Console = await Shared.HttpRequest(step.ConsoleUri);

                    updated = true;

                    if (step.Name.ToLower().Contains("helix") && step.Console.Contains("Waiting for completion of job "))
                    {
                        step.IsHelixSubmission = true;
                    }
                }
            }

            if (step.Console != null)
            {
                string lowerCase = step.Console.ToLower();
                bool isHelixSubmission = lowerCase.Contains("helix") && step.Console.Contains("Waiting for completion of job ");

                if (step.IsHelixSubmission != isHelixSubmission)
                {
                    Debug.Assert(step.Console.Contains("Waiting for completion of job "));

                    step.IsHelixSubmission = isHelixSubmission;
                    updated = true;
                }
            }

            if (step.IsHelixSubmission && step.HelixModel == null)
            {
                if (step.Console == null)
                {
                    continue;
                }

                Debug.Assert(step.Console != null);

                // Parse the console uri for the workitems
                var split = step.Console.Split("Waiting for completion of job ");

                List<string> jobs = new List<string>();
                bool first = true;
                foreach (var item in split)
                {
                    if (first)
                    {
                        first = false;
                        continue;
                    }

                    jobs.Add(item.Split("\n")[0].Trim());
                }

                if (step.Id == null)
                {
                    step.Id = Guid.NewGuid().ToString();
                    updated = true;
                }

                DateTime beginTime = DateTime.Now;
                HelixIO io = new HelixIO(GlobalLock, HelixContainer, jobs, step.Name, step.Id);
                step.HelixModel = await io.IngestData();
                updated = true;

                foreach (var item in step.HelixModel)
                {
                    foreach (var workItemModel in item.WorkItems)
                    {
                        workItemModel.Console = null;
                    }
                }

                step.HelixModel = null;
                DateTime endTime = DateTime.Now;

                double totalSeconds = (endTime - beginTime).TotalSeconds;
                
                Console.WriteLine("------------------------------------------------------");
                Console.WriteLine($"[{++DownloadedJobs}]: Helix workItems: {totalSeconds}");
            }
        }

        if (updated)
        {
            await ReplaceOperation(document);
        }
    }

    private async Task ReplaceOperation(AzureDevOpsJobModel document)
    {
        foreach (var step in document.Steps)
        {
            if (step.Console != null)
            {
                DocumentSize += step.Console.Length;
            }
        }

        if (DocumentSize > CapSize || CosmosOperations.Count > 190)
        {
            int retryCount = CosmosOperations.Count;
            await DrainCosmosOperations();
            DocumentSize = 0;
            CosmosOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        }

        CosmosOperations.Add(Container.ReplaceItemAsync<AzureDevOpsJobModel>(document, document.Id, new PartitionKey(document.Name)).CaptureOperationResponse(document));
        ++ ActiveTasks;
    }

    private async Task RetryReplaceOperation(AzureDevOpsJobModel document)
    {
        foreach (var step in document.Steps)
        {
            if (step.Console != null)
            {
                RetrySize += step.Console.Length;
            }
        }

        if (RetrySize > CapSize || CosmosRetryOperations.Count > 50)
        {
            int retryCount = CosmosRetryOperations.Count;
            await DrainRetryOperations();
            RetrySize = 0;
            CosmosRetryOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        }

        CosmosRetryOperations.Add(Container.ReplaceItemAsync<AzureDevOpsJobModel>(document, document.Id, new PartitionKey(document.Name)).CaptureOperationResponse(document));
        ++ ActiveTasks;
    }

    private async Task DrainCosmosOperations()
    {
        ActiveTasks -= CosmosOperations.Count;

        BulkOperationResponse<AzureDevOpsJobModel> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(CosmosOperations);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        CosmosOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await RetryReplaceOperation(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

    private async Task DrainRetryOperations()
    {
        ActiveTasks -= CosmosOperations.Count;

        BulkOperationResponse<AzureDevOpsJobModel> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(CosmosOperations);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        CosmosOperations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await RetryReplaceOperation(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

}