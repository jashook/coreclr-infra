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
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;

using models;
using DevOps.Util;

using ev27;

////////////////////////////////////////////////////////////////////////////////

public class JobIO
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public JobIO(Database db)
    {   
        HelixContainer = db.GetContainer("helix-jobs");

        DownloadedJobs = 0;

        lock(UploadLock)
        {
            if (Uploader == null)
            {
                Uploader = new CosmosUpload<AzureDevOpsJobModel>(GlobalLock, HelixContainer, Queue);
            }
        }

    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    private Container HelixContainer { get; set; }
    public long DownloadedJobs { get; set; }
    public List<string> Jobs { get; set; }

    private static TreeQueue<AzureDevOpsJobModel> Queue = new TreeQueue<AzureDevOpsJobModel>(maxLeafSize: 50);
    private static CosmosUpload<AzureDevOpsJobModel> Uploader = null;
    private static object UploadLock = new object();
    private static object GlobalLock = new object();

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public async Task UploadData(List<AzureDevOpsJobModel> jobs)
    {
        List<Task> tasks = new List<Task>();
        foreach (AzureDevOpsJobModel document in jobs)
        {
            tasks.Add(UploadDocument(document, true, false));

            if (tasks.Count > 5)
            {
                await Task.WhenAll(tasks);
            }
        }

        await Task.WhenAll(tasks);

        HelixIO.Finishing = true;
        Uploader.Finish();
    }

    public async Task ReUploadData(FeedIterator<AzureDevOpsJobModel> iterator, bool force)
    {
        int procCount = Environment.ProcessorCount;
        List<Task> tasks = new List<Task>();
        while (iterator.HasMoreResults)
        {
            var cosmosResult = await iterator.ReadNextAsync();
            IEnumerable<AzureDevOpsJobModel> items = cosmosResult.Take(cosmosResult.Resource.Count());

            foreach (AzureDevOpsJobModel document in items)
            {
                tasks.Add(UploadDocument(document, false, force));

                if (tasks.Count > 5)
                {
                    await Task.WhenAll(tasks);
                }
            }

            await Task.WhenAll(tasks);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////

    private async Task UploadDocument(AzureDevOpsJobModel document, bool forceInsert, bool forceDownloadConsole)
    {
        bool updated = false;
        List<Task> tasks = new List<Task>();
        
        foreach (AzureDevOpsStepModel step in document.Steps)
        {
            tasks.Add(Task.Run(() => {
                DateTime beginTime = DateTime.Now;
                double elapsedUriDownloadTime = 0;
                if ((step.ConsoleUri != null) && 
                    ((forceDownloadConsole == true || 
                    step.Result == TaskResult.Failed || 
                    step.Name == "Initialize containers" ||
                    step.Name.ToLower().Contains("helix"))))
                {
                    if (step.Console != null && forceDownloadConsole == true)
                    {
                        updated = true;
                    }

                    DateTime beginUriDownload = DateTime.Now;
                    step.Console = Shared.Get(step.ConsoleUri);
                    DateTime endUriDownload = DateTime.Now;
                    elapsedUriDownloadTime = (endUriDownload - beginUriDownload).TotalMilliseconds;

                    if (step.Name.ToLower().Contains("helix") && step.Console.Contains("Waiting for completion of job "))
                    {
                        step.IsHelixSubmission = true;
                    }

                    if (forceDownloadConsole != true && step.Result != TaskResult.Failed && step.Name != "Initialize containers" && !step.IsHelixSubmission)
                    {
                        // We do not want to save console logs for successful jobs
                        step.Console = null;
                    }
                }

                if (step.IsHelixSubmission && step.HelixModel == null)
                {
                    if (step.Console == null)
                    {
                        return;
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

                    DateTime helixBeginTime = DateTime.Now;
                    HelixIO io = new HelixIO(HelixContainer, jobs, step.Name, step.Id);
                    var task = io.IngestData();
                    task.Wait();
                    step.HelixModel = task.Result;
                    updated = true;

                    foreach (var item in step.HelixModel)
                    {
                        foreach (var workItemModel in item.WorkItems)
                        {
                            workItemModel.Console = null;
                        }
                    }

                    step.HelixModel = null;
                    DateTime helixEndTime = DateTime.Now;

                    double totalSeconds = (helixEndTime - helixBeginTime).TotalMilliseconds;
                    Console.WriteLine($"[{++DownloadedJobs}]: Helix workItems: {totalSeconds}");
                }

                DateTime endTime = DateTime.Now;
                double elapsedTime = (endTime - beginTime).TotalMilliseconds;

                bool logAll = false;
                if (elapsedTime > 1000 | logAll)
                {
                    Console.WriteLine($"[{step.Name}]: Processed in {elapsedTime}. Downloaded console in {elapsedUriDownloadTime}.");
                }
            }));
        }

        DateTime jobStarted = DateTime.Now;
        await Task.WhenAll(tasks);
        DateTime jobEnded = DateTime.Now;

        double elapsedJobTime = (jobEnded - jobStarted).TotalMilliseconds;

        Console.WriteLine($"[Job] -- [{document.Name}]: Processed job in {elapsedJobTime}ms.");

        if (updated || forceInsert)
        {
            SubmitToUpload(document);
        }
    }

    private void SubmitToUpload(AzureDevOpsJobModel model)
    {
        lock(UploadLock)
        {
            Queue.Enqueue(model);
        }
    }
}