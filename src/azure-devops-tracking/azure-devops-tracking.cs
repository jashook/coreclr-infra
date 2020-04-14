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

    public AzureDevopsTracking(bool recreateDb=false)
    {
        SetupDatabase().Wait();
        SetupCollection(recreateDb).Wait();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Public member methods
    ////////////////////////////////////////////////////////////////////////////

    public async Task Remove(DateTime dateToStartRemoving)
    {
        int modelDeletedCount = 1;
        int modelFailedDeleteCount = 0;
        int jobDeletedCount = 1;
        int jobFailedDeleteCount = 0;

        // try
        // {
        //     var queryable = RuntimeContainer.GetItemLinqQueryable<RuntimeModel>();
        //     var query = queryable.Where(item => item.DateStart > dateToStartRemoving);

        //     FeedIterator<RuntimeModel> iterator = query.ToFeedIterator();

        //     while (iterator.HasMoreResults)
        //     {
        //         var cosmosResult = await iterator.ReadNextAsync();
                
        //         IEnumerable<RuntimeModel> items = cosmosResult.Take(cosmosResult.Resource.Count());
                
        //         int totalCount = items.Count();
        //         foreach (var item in items)
        //         {
        //             Console.WriteLine($"[{modelDeletedCount++}:{totalCount}] Deleting.");

        //             try
        //             {
        //                 await RuntimeContainer.DeleteItemAsync<RuntimeModel>(item.Id, new PartitionKey(item.BuildReasonString));
        //             }
        //             catch (Exception e)
        //             {
        //                 ++modelFailedDeleteCount;
        //                 Console.Write(e.ToString());
        //             }
        //         }
        //     }
        // }
        // catch (Exception e)
        // {
        //     Console.Write(e);5
        // }

        try
        {
            var queryable = JobContainer.GetItemLinqQueryable<AzureDevOpsJobModel>();
            var query = queryable.Where(item => item.DateStart > dateToStartRemoving);

            FeedIterator<AzureDevOpsJobModel> iterator = query.ToFeedIterator();

            while (iterator.HasMoreResults)
            {
                var cosmosResult = await iterator.ReadNextAsync();

                IEnumerable<AzureDevOpsJobModel> items = cosmosResult.Take(cosmosResult.Resource.Count());
            
                int totalCount = items.Count();
                foreach (var item in items)
                {
                    Console.WriteLine($"[{jobDeletedCount++}:{totalCount}] Deleting.");

                    bool found = false;
                    try
                    {
                        // Read the item to see if it exists
                        ItemResponse<AzureDevOpsJobModel> response = await JobContainer.ReadItemAsync<AzureDevOpsJobModel>(id: item.Id, partitionKey: new PartitionKey(item.Name));
                        found = true;
                    }
                    catch(CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                    {
                        found = false;
                    }

                    Debug.Assert(found);
                    
                    try 
                    {
                        await JobContainer.DeleteItemAsync<AzureDevOpsJobModel>(item.Name, new PartitionKey(item.Name));
                    }
                    catch (Exception e)
                    {
                        Console.Write(e.ToString());
                    }
                }
            }
        }
        catch (Exception e)
        {
            Console.Write(e);
            ++jobFailedDeleteCount;
        }

        Console.WriteLine("Models:");
        Console.WriteLine($"Deleted: {modelDeletedCount}, Failed to Delete: {modelFailedDeleteCount}");
        Console.WriteLine("Jobs:");
        Console.WriteLine($"Deleted: {jobDeletedCount}, Failed to Delete: {jobFailedDeleteCount}");
    }

    public async Task Update()
    {
        int limit = 200;

        var lastRun = await GetLastRunFromDb();

        List<Build> builds = await Server.ListBuildsAsync("public", new int[] {
            686
        });

        var runs = await GetRunsSince(lastRun, builds, limit);

        int runsUploaded = 0;
        while (runs.Count > 0)
        {
            runsUploaded += runs.Count;
            await UploadRuns(runs);
            lastRun = await GetLastRunFromDb();

            if (runs.Count % (limit * 5) == 0)
            {
                builds = await Server.ListBuildsAsync("public", new int[] {
                    686
                });
            }

            runs = await GetRunsSince(lastRun, builds, limit);
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

    private async Task<List<RuntimeModel>> GetRunsSince(RuntimeModel lastRun, List<Build> builds, int limit = -1)
    {
        Debug.Assert(builds != null);

        List<Build> filteredBuilds = new List<Build>();
        if (lastRun != null)
        {
            var lastStartDateToFilter = lastRun.DateStart;

            foreach (var item in builds)
            {
                if (item.StartTime == null) continue;

                if (DateTime.Parse(item.StartTime) > lastStartDateToFilter)
                {
                    filteredBuilds.Add(item);
                }
            }
        }
        else
        {
            foreach(var item in builds)
            {
                filteredBuilds.Add(item);
            }
        }

        List<RuntimeModel> models = new List<RuntimeModel>();
        List<Build> buildRecords = new List<Build>();
        foreach (var item in filteredBuilds)
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
            model.BuildReasonString = build.Reason.ToString();
            
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

        int modelConflicts = 0;
        while (modelQueue.Count != 0)
        {
            var model = modelQueue.Dequeue();
            model.Id = model.BuildNumber;

            foreach (var item in model.Jobs)
            {
                // Use random value for ID
                item.Id = Guid.NewGuid().ToString();
                item.PipelineId = model.Id;

                jobQueue.Enqueue(item);
            }

            // Zero out the data for the jobs, they will be uploaded seperately
            model.Jobs = null;

            try
            {
                await RuntimeContainer.CreateItemAsync<RuntimeModel>(model, new PartitionKey(model.BuildReasonString));
            }
            catch (Exception e)
            {
                ++modelConflicts;
            }
        }

        int itemNumber = 1;
        int totalCount = jobQueue.Count;
        while (jobQueue.Count != 0)
        {
            var jobModel = jobQueue.Dequeue();

            var unModifiedId = jobModel.Id;
            int conflictCount = 1;
            try
            {
                Console.WriteLine($"[{itemNumber++}:{totalCount}] Uploading job.");
                await JobContainer.CreateItemAsync<AzureDevOpsJobModel>(jobModel, new PartitionKey(jobModel.Name));
            }
            catch (Exception e)
            {
                // Unable to upload the item, keep trying.

                jobModel.Id = $"{unModifiedId}-{conflictCount++}";
                jobQueue.Enqueue(jobModel);

                ++conflicts;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Private setup methods
    ////////////////////////////////////////////////////////////////////////////

    public async Task SetupDatabase(bool deleteData=false)
    {
        this.Db = await Client.CreateDatabaseIfNotExistsAsync(DatabaseName);
    }

    public async Task SetupCollection(bool deleteData=false)
    {
        if (deleteData)
        {
            await Db.GetContainer(RuntimeContainerName).DeleteContainerAsync();
            await Db.GetContainer(JobContainerName).DeleteContainerAsync();
        }

        ContainerProperties runtimeContainerProperties = new ContainerProperties(RuntimeContainerName, partitionKeyPath: "/BuildReasonString");
        ContainerProperties jobContainerProperties = new ContainerProperties(JobContainerName, partitionKeyPath: "/Name");

        this.RuntimeContainer = await Db.CreateContainerIfNotExistsAsync(runtimeContainerProperties, throughput: 400);
        this.JobContainer = await Db.CreateContainerIfNotExistsAsync(jobContainerProperties, throughput: 1000);
    }

}