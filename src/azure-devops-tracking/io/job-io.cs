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

        CreatedHelixJobs = false;

        lock(UploadLock)
        {
            if (Uploader == null)
            {
                Func<AzureDevOpsJobModel, string> getPartitionKey = (AzureDevOpsJobModel document) => { return document.Name; };
                Action<AzureDevOpsJobModel> trimDoc = (AzureDevOpsJobModel document) => {
                    long roughMaxSize = 1800000; // 2,000,000 bytes (2mb)

                    int maxStepSize = (int)Math.Floor((double)(roughMaxSize / document.Steps.Count));

                    foreach (var step in document.Steps)
                    {
                        if (step.Console != null)
                        {
                            int maxIndex = maxStepSize - 1;

                            if (step.Console.Length > maxIndex)
                            {
                                step.Console = step.Console.Substring(0, maxIndex);
                            }
                        }
                    }

                    Debug.Assert(document.ToString().Length < CosmosUpload<AzureDevOpsJobModel>.CapSize);
                };

                Queue = new TreeQueue<AzureDevOpsJobModel>(maxLeafSize: 50);
                Uploader = new CosmosUpload<AzureDevOpsJobModel>("[Azure Dev Ops Job Model Upload]", GlobalLock, db.GetContainer("runtime-jobs"), Queue, getPartitionKey, trimDoc, waitForUpload: true);
            }
        }

    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    private Container HelixContainer { get; set; }
    public long DownloadedJobs { get; set; }
    public bool CreatedHelixJobs { get; set; }
    public List<string> Jobs { get; set; }

    private static TreeQueue<AzureDevOpsJobModel> Queue = null;
    private static CosmosUpload<AzureDevOpsJobModel> Uploader = null;
    private static object UploadLock = new object();
    private static object GlobalLock = new object();

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public async Task UploadData(Queue<AzureDevOpsJobModel> jobs)
    {
        List<Task> tasks = new List<Task>();
        int count = 0;
        int total = jobs.Count;
        DateTime beginTime = DateTime.Now;
        while (jobs.Count > 0)
        {
            AzureDevOpsJobModel document = jobs.Dequeue();
            ++count;
            tasks.Add(UploadDocument(document, true, false));

            if (tasks.Count > 500)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
            }

            Console.WriteLine($"[Job Count] - [{count}:{total}] - Finished");
        }

        await Task.WhenAll(tasks);
        Console.WriteLine($"[Job Count] - [{total}:{total}] - Finished");

        DateTime endTime = DateTime.Now;
        double elapsedTime = (endTime - beginTime).TotalMinutes;
        Console.WriteLine($"Processed {total} jobs in {elapsedTime}m");

        if (CreatedHelixJobs)
        {
            HelixIO.SignalToFinish();
        }

        Uploader.Finish();
        Uploader = null;

        Debug.Assert(Uploader == null);
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
        List<Task> tasks = new List<Task>();
        
        foreach (AzureDevOpsStepModel step in document.Steps)
        {
            tasks.Add(UploadStep(step, forceDownloadConsole));
        }

        DateTime jobStarted = DateTime.Now;
        await Task.WhenAll(tasks);
        DateTime jobEnded = DateTime.Now;

        double elapsedJobTime = (jobEnded - jobStarted).TotalMilliseconds;

        Console.WriteLine($"[Job] -- [{document.Name}]: Processed job in {elapsedJobTime}ms.");
        SubmitToUpload(document);
    }

    private async Task UploadStep(AzureDevOpsStepModel step, bool forceDownloadConsole)
    {
        DateTime beginTime = DateTime.Now;
        double elapsedUriDownloadTime = 0;
        if ((step.ConsoleUri != null) && 
            ((forceDownloadConsole == true || 
            step.Result == TaskResult.Failed || 
            step.Name == "Initialize containers" ||
            step.Name.ToLower().Contains("helix"))))
        {
            DateTime beginUriDownload = DateTime.Now;
            step.Console = null;

            try
            {
                step.Console = await Shared.GetAsync(step.ConsoleUri, retryCount: 10);
            }
            catch(Exception e)
            {
                // Unable to download console
            }

            DateTime endUriDownload = DateTime.Now;
            elapsedUriDownloadTime = (endUriDownload - beginUriDownload).TotalMilliseconds;

            bool containsHelixSubmissions = false;
            if (step.Console != null)
            {
                if (step.Console.Contains("Waiting for completion of job "))
                {
                    containsHelixSubmissions = true;
                }
            }

            if (step.Name.ToLower().Contains("helix") && containsHelixSubmissions)
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
            }

            TimeSpan passedTime = DateTime.Now - step.DateStart;
            if (passedTime.Days <= 8)
            {
                DateTime helixBeginTime = DateTime.Now;
                HelixIO io = new HelixIO(HelixContainer, jobs, step.Name, step.Id);

                CreatedHelixJobs = true;
                var task = io.IngestData();
                task.Wait();
                step.HelixModel = task.Result;

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
        }

        DateTime endTime = DateTime.Now;
        double elapsedTime = (endTime - beginTime).TotalMilliseconds;

        bool logAll = false;
        if (elapsedTime > 1000 | logAll)
        {
            Console.WriteLine($"[{step.Name}]: Processed in {elapsedTime}. Downloaded console in {elapsedUriDownloadTime}.");
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