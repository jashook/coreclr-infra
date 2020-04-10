////////////////////////////////////////////////////////////////////////////////
//
// Module: azure-devops-tracking.cs
//
// Notes:
//
// Attempt to track everything that is being produced in azure dev ops.
//
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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using models;
using DevOps.Util;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class AzureDevopsTracking
{
    ////////////////////////////////////////////////////////////////////////////
    // Member variables.
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // Private member variables.
    ////////////////////////////////////////////////////////////////////////////

    private async Task HttpRequest(string location)
    {
        using (WebClient client = new WebClient())
        {
            client.DownloadStringCompleted += new DownloadStringCompletedEventHandler((sender, eventArgs) => {
                string text = eventArgs.Result;
            });

            await client.DownloadStringTaskAsync(new Uri(location));
        }
    }

    private async Task HttpRequest(string location, Action<string> handleResponse)
    {
        using (WebClient client = new WebClient())
        {
            client.DownloadStringCompleted += new DownloadStringCompletedEventHandler((sender, eventArgs) => {
                string text = eventArgs.Result;

                handleResponse(text);
            });

            await client.DownloadStringTaskAsync(new Uri(location));
        }
    }

    private static DevOpsServer _devopsServer;
    private DevOpsServer Server { 
        get
        {
            if (_devopsServer == null)
            {
                _devopsServer = new DevOpsServer("dnceng", Environment.GetEnvironmentVariable("dncengKey"));
            }

            return _devopsServer;
        } 
        set { }
    }

    private static readonly string EndpointUri = "https://coreclr-infra.documents.azure.com:443/";
    private static readonly string PrimaryKey = Environment.GetEnvironmentVariable("coreclrInfraKey");

    private CosmosClient _client;
    private CosmosClient Client {
        get
        {
            if (_client == null)
            {
                _client = new CosmosClient(EndpointUri, PrimaryKey);
            }

            return _client;
        }
        set { }
    }

    private Database Db;
    private Container RuntimeContainer;
    private Container JobContainer;

    private static readonly string DatabaseName = "coreclr-infra";
    private static readonly string RuntimeContainerName = "runtime-pipelines";
    private static readonly string JobContainerName = "runtime-jobs";

    private static int conflicts = 0;

    ////////////////////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////////////////////

    public AzureDevopsTracking()
    {
        SetupDatabase().Wait();
        SetupCollection().Wait();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Public member methods
    ////////////////////////////////////////////////////////////////////////////

    public async Task Update()
    {
        int limit = 40;

        var lastRun = await GetLastRunFromDb();
        var runs = await GetRunsSince(lastRun, limit);

        while (runs.Count > 0)
        {
            await UploadRuns(runs);

            bool retry = false;
            int backoffAmount = 1000;
            int count = 1;
            do
            {
                try
                {
                    lastRun = await GetLastRunFromDb();
                }
                catch (Exception e)
                {
                    int waitTime = backoffAmount * count++;
                    Console.WriteLine($"Failed to get last run. Waiting {waitTime}");
                    retry = true;
                    Thread.Sleep(waitTime);
                }
            } while (retry);

            runs = await GetRunsSince(lastRun, limit);
        }

        Debug.Assert(runs.Count == 0);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Private member methods
    ////////////////////////////////////////////////////////////////////////////

    private async Task<RuntimeModel> GetLastRunFromDb()
    {
        RuntimeModel model = null;

        try
        {
            var queryable = RuntimeContainer.GetItemLinqQueryable<RuntimeModel>();
            var query = queryable.OrderByDescending(item => item.DateStart).Take(1);

            FeedIterator<RuntimeModel> iterator = query.ToFeedIterator();

            var cosmosResult = await iterator.ReadNextAsync();

            if (cosmosResult.Resource.Count() != 0)
            {
                model = cosmosResult.Resource.First();
            }
        }
        catch (Exception e)
        {
            Console.Write(e);
        }

        return model;
    }

    private async Task<List<RuntimeModel>> GetRunsSince(RuntimeModel lastRun, int limit = -1)
    {
        var builds = await Server.ListBuildsAsync("public", new int[] {
            686
        });

        if (lastRun != null)
        {
            List<Build> filteredBuilds = new List<Build>();
            var lastStartDateToFilter = lastRun.DateStart;

            foreach (var item in builds)
            {
                if (item.StartTime == null) continue;

                if (DateTime.Parse(item.StartTime) > lastStartDateToFilter)
                {
                    filteredBuilds.Add(item);
                }
            }

            builds = filteredBuilds;
        }

        List<RuntimeModel> models = new List<RuntimeModel>();
        List<Build> buildRecords = new List<Build>();
        foreach (var item in builds)
        {
            buildRecords.Add(item);
        }

        buildRecords.RemoveAll(item => item.StartTime == null);
        buildRecords.RemoveAll(item => item.FinishTime == null);
        buildRecords.Sort((lhs, rhs) => DateTime.Parse(lhs.StartTime) < DateTime.Parse(rhs.StartTime) ? -1 : 1);

        int total = buildRecords.Count;
        int index = 1;
        foreach (var build in buildRecords)
        {
            if (limit > 0 && index > limit)
            {
                break;
            }

            Console.WriteLine($"[{index++}:{total}]");

            RuntimeModel model = new RuntimeModel();
            if (build.FinishTime == null)
            {
                continue;
            }

            model.BuildNumber = build.BuildNumber;
            model.DateEnd = DateTime.Parse(build.FinishTime);
            model.DateStart = DateTime.Parse(build.StartTime);
            model.ElapsedTime = (model.DateEnd - model.DateStart).Seconds;
            model.BuildResult = build.Result;
            model.BuildNumber = build.BuildNumber;
            model.BuildReason = build.Reason;
            
            if (build.Reason == BuildReason.PullRequest)
            {
                var dict = JObject.FromObject(build.TriggerInfo).ToObject<Dictionary<string, string>>();;
                
                model.PrNumber = dict["pr.number"];
                model.PrSourceBranch = dict["pr.sourceBranch"];
                model.PrSourceSha = dict["pr.sourceSha"];
                model.PrTitle = dict["pr.title"];
                model.PrSenderName = dict["pr.sender.name"];
            }

            model.SourceSha = build.SourceVersion;

            var timeline = await Server.GetTimelineAsync(build.Project.Name, build.Id);

            if (timeline is null)
            {
                continue;
            }

            Dictionary<string, AzureDevOpsJobModel> jobs = new Dictionary<string, AzureDevOpsJobModel>();
            Dictionary<string, TimelineRecord> records = new Dictionary<string, TimelineRecord>();

            int count = 0;
            foreach (var record in timeline.Records)
            {
                records[record.Id] = record;

                if (record.FinishTime == null)
                {
                    continue;
                }

                var step = new AzureDevOpsStepModel();

                if (record.StartTime == null)
                {
                    continue;
                }

                step.DateStart = DateTime.Parse(record.StartTime);
                step.DateEnd = DateTime.Parse(record.FinishTime);
                step.ElapsedTime = (step.DateEnd - step.DateStart).TotalSeconds;
                step.Result = record.Result;
                step.Name = record.Name;
                step.Machine = record.WorkerName;
                step.StepGuid = record.Id;
                step.ParentGuid = record.ParentId;

                if (record.Log != null)
                {
                    step.ConsoleUri = record.Log.Url;

                    if (step.Name.Contains("Evaluate paths for"))
                    {
                        await HttpRequest(step.ConsoleUri, async (htmlResponse) =>
                        {
                            step.Console = htmlResponse;
                        });
                    }
                }

                if (record.ParentId == null)
                {
                    count += 1;
                    continue;
                }
                
                if (jobs.ContainsKey(record.ParentId))
                {
                    AzureDevOpsJobModel jobModel = jobs[record.ParentId];

                    jobModel.Steps.Add(step);
                }
                else if (jobs.ContainsKey(record.Id))
                {
                    // Continue.

                    continue;
                }
                else
                {
                    AzureDevOpsJobModel jobModel = new AzureDevOpsJobModel();

                    jobModel.Steps = new List<AzureDevOpsStepModel>();
                    jobModel.Steps.Add(step);

                    jobs[record.ParentId] = jobModel;
                }
            }

            List<AzureDevOpsJobModel> jobList = new List<AzureDevOpsJobModel>();
            foreach (var kv in jobs)
            {
                AzureDevOpsJobModel jobModel = kv.Value;

                // Populate the jobModel
                Debug.Assert(records.ContainsKey(kv.Key));
                var record = records[kv.Key];

                if (record.StartTime == null) continue;

                jobModel.DateEnd = DateTime.Parse(record.FinishTime);
                jobModel.DateStart = DateTime.Parse(record.StartTime);
                jobModel.ElapsedTime = (jobModel.DateEnd - jobModel.DateStart).TotalMinutes;
                
                jobModel.JobGuid = record.Id;

                Debug.Assert(record.Id == kv.Key);

                jobModel.Name = record.Name;
                jobModel.Result = record.Result;

                jobModel.Steps.Sort(new StepComparer());

                jobList.Add(jobModel);
            }

            jobList.Sort(new JobComparer());
            model.Jobs = jobList;

            AzureDevOpsJobModel checkoutJob = null;
            foreach (var job in model.Jobs)
            {
                if (job.Name.ToLower() == "checkout")
                {
                    checkoutJob = job;
                    break;
                }
            }

            if (checkoutJob != null)
            {
                foreach (var step in checkoutJob.Steps)
                {
                    if (step.Console != null)
                    {
                        if (step.Name.ToLower().Contains("coreclr"))
                        {
                            if (step.Console.Contains("No changed files for"))
                            {
                                model.IsCoreclrRun = false;
                            }
                            else
                            {
                                model.IsCoreclrRun = true;
                            }
                        }
                        else if (step.Name.ToLower().Contains("libraries"))
                        {
                            if (step.Console.Contains("No changed files for"))
                            {
                                model.IsLibrariesRun = false;
                            }
                            else
                            {
                                model.IsLibrariesRun = true;
                            }
                        }
                        else if (step.Name.ToLower().Contains("installer"))
                        {
                            if (step.Console.Contains("No changed files for"))
                            {
                                model.IsInstallerRun = false;
                            }
                            else
                            {
                                model.IsInstallerRun = true;
                            }
                        }
                        else if (step.Name.ToLower().Contains("mono"))
                        {
                            if (step.Console.Contains("No changed files for"))
                            {
                                model.IsMonoRun = false;
                            }
                            else
                            {
                                model.IsMonoRun = true;
                            }
                        }
                        else
                        {
                            // Unreached
                            Debug.Assert(false);
                        }
                    }
                }
            }

            models.Add(model);
        }

        return models;
    }

    private async Task UploadRuns(List<RuntimeModel> models)
    {
        Queue<RuntimeModel> modelQueue = new Queue<RuntimeModel>();
        Queue<AzureDevOpsJobModel> jobQueue = new Queue<AzureDevOpsJobModel>();

        foreach (var item in models)
        {
            modelQueue.Enqueue(item);
        }

        while (modelQueue.Count != 0)
        {
            var model = modelQueue.Dequeue();
            model.Id = model.BuildNumber;

            foreach (var item in model.Jobs)
            {
                item.Id = item.JobGuid + item.DateStart.ToString() + item.DateEnd.ToString();
                item.PipelineId = model.Id;

                jobQueue.Enqueue(item);
            }

            // Zero out the data for the jobs, they will be uploaded seperately
            model.Jobs = null;

            bool found = false;
            try
            {
                // Read the item to see if it exists
                ItemResponse<RuntimeModel> response = await RuntimeContainer.ReadItemAsync<RuntimeModel>(model.Id, new PartitionKey(model.Id));
                found = true;
            }
            catch(CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                found = false;
            }

            if (found)
            {
                Debug.Assert(found);
            }

            else
            {
                try
                {
                    await RuntimeContainer.CreateItemAsync<RuntimeModel>(model, new PartitionKey(model.Id));
                }
                catch (Exception e)
                {
                    // Unable to upload the item, keep trying.
                    modelQueue.Enqueue(model);
                    
                    // Wait a time bit.
                    Thread.Sleep(500);
                }
            }
        }

        int itemNumber = 1;
        int totalCount = jobQueue.Count;
        while (jobQueue.Count != 0)
        {
            var jobModel = jobQueue.Dequeue();
            try
            {
                Console.WriteLine($"[{itemNumber++}:{totalCount}] Uploading job.");
                await JobContainer.CreateItemAsync<AzureDevOpsJobModel>(jobModel, new PartitionKey(jobModel.Id));
            }
            catch (Exception e)
            {
                // Unable to upload the item, keep trying.

                ++conflicts;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Private setup methods
    ////////////////////////////////////////////////////////////////////////////

    public async Task SetupDatabase()
    {
        this.Db = await Client.CreateDatabaseIfNotExistsAsync(DatabaseName);
    }

    public async Task SetupCollection()
    {
        this.RuntimeContainer = await Db.CreateContainerIfNotExistsAsync(RuntimeContainerName, "/id");
        this.JobContainer = await Db.CreateContainerIfNotExistsAsync(JobContainerName, "/id");
    }

}