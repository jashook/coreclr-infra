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

using Microsoft.Extensions.Configuration;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using models;
using DevOps.Util;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// </ResponseType>
public class BulkOperationResponse<T>
{
    public TimeSpan TotalTimeTaken { get; set; }
    public int SuccessfulDocuments { get; set; } = 0;
    public double TotalRequestUnitsConsumed { get; set; } = 0;

    public IReadOnlyList<(T, Exception)> Failures { get; set; }
}
// </ResponseType>

// <OperationResult>
public class OperationResponse<T>
{
    public T Item { get; set; }
    public double RequestUnitsConsumed { get; set; } = 0;
    public bool IsSuccessful { get; set; }
    public Exception CosmosException { get; set; }
}
// </OperationResult>

public static class TaskExtensions
{
    // <CaptureOperationResult>
    public static Task<OperationResponse<T>> CaptureOperationResponse<T>(this Task<ItemResponse<T>> task, T item)
    {
        return task.ContinueWith(itemResponse =>
        {
            if (itemResponse.IsCompletedSuccessfully)
            {
                return new OperationResponse<T>()
                {
                    Item = item,
                    IsSuccessful = true,
                    RequestUnitsConsumed = task.Result.RequestCharge
                };
            }

            AggregateException innerExceptions = itemResponse.Exception.Flatten();
            CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
            if (cosmosException != null)
            {
                return new OperationResponse<T>()
                {
                    Item = item,
                    RequestUnitsConsumed = cosmosException.RequestCharge,
                    IsSuccessful = false,
                    CosmosException = cosmosException
                };
            }

            return new OperationResponse<T>()
            {
                Item = item,
                IsSuccessful = false,
                CosmosException = innerExceptions.InnerExceptions.FirstOrDefault()
            };
        });
    }
    // </CaptureOperationResult>
}

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
                _client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
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

        try
        {
            var queryable = RuntimeContainer.GetItemLinqQueryable<RuntimeModel>();
            var query = queryable.Where(item => item.DateStart > dateToStartRemoving);

            FeedIterator<RuntimeModel> iterator = query.ToFeedIterator();

            while (iterator.HasMoreResults)
            {
                var cosmosResult = await iterator.ReadNextAsync();
                
                IEnumerable<RuntimeModel> items = cosmosResult.Take(cosmosResult.Resource.Count());
                
                int totalCount = items.Count();
                foreach (var item in items)
                {
                    Console.WriteLine($"[{modelDeletedCount++}:{totalCount}] Deleting.");

                    try
                    {
                        await RuntimeContainer.DeleteItemAsync<RuntimeModel>(item.Id, new PartitionKey(item.BuildReasonString));
                    }
                    catch (Exception e)
                    {
                        ++modelFailedDeleteCount;
                        Console.Write(e.ToString());
                    }
                }
            }
        }
        catch (Exception e)
        {
            Console.Write(e);
        }

        try
        {
            var queryable = JobContainer.GetItemLinqQueryable<AzureDevOpsJobModel>();
            var query = queryable.Where(item => item.DateStart > dateToStartRemoving);

            FeedIterator<AzureDevOpsJobModel> iterator = query.ToFeedIterator();

            List<Task<OperationResponse<AzureDevOpsJobModel>>> retries = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();

            while (iterator.HasMoreResults)
            {
                var cosmosResult = await iterator.ReadNextAsync();

                IEnumerable<AzureDevOpsJobModel> items = cosmosResult.Take(cosmosResult.Resource.Count());

                // <BulkDelete>
                List<Task<OperationResponse<AzureDevOpsJobModel>>> operations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>(items.Count());
                foreach (AzureDevOpsJobModel document in items)
                {
                    operations.Add(JobContainer.DeleteItemAsync<AzureDevOpsJobModel>(document.Id, new PartitionKey(document.Name)).CaptureOperationResponse(document));
                }
                // </BulkDelete>

                BulkOperationResponse<AzureDevOpsJobModel> bulkOperationResponse = await ExecuteTasksAsync(operations);
                Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
                Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                Console.WriteLine($"Deleted {bulkOperationResponse.SuccessfulDocuments} documents");
                Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
                if (bulkOperationResponse.Failures.Count > 0)
                {
                    Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


                    foreach (var item in bulkOperationResponse.Failures)
                    {
                        retries.Add(JobContainer.DeleteItemAsync<AzureDevOpsJobModel>(item.Item1.Id, new PartitionKey(item.Item1.Name)).CaptureOperationResponse(item.Item1));
                    }
                }

                jobDeletedCount += bulkOperationResponse.SuccessfulDocuments;
            }

            while (retries.Count > 0)
            {
                BulkOperationResponse<AzureDevOpsJobModel> bulkOperationResponse = await ExecuteTasksAsync(retries);
                Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
                Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                Console.WriteLine($"Deleted {bulkOperationResponse.SuccessfulDocuments} documents");
                Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
                if (bulkOperationResponse.Failures.Count > 0)
                {
                    Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


                    foreach (var item in bulkOperationResponse.Failures)
                    {
                        retries.Add(JobContainer.DeleteItemAsync<AzureDevOpsJobModel>(item.Item1.Id, new PartitionKey(item.Item1.Name)).CaptureOperationResponse(item.Item1));
                    }
                }

                jobDeletedCount += bulkOperationResponse.SuccessfulDocuments;
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
    // Private types
    ////////////////////////////////////////////////////////////////////////////

    

    ////////////////////////////////////////////////////////////////////////////
    // Private member methods
    ////////////////////////////////////////////////////////////////////////////

    private static async Task<BulkOperationResponse<T>> ExecuteTasksAsync<T>(IReadOnlyList<Task<OperationResponse<T>>> tasks)
    {
        // <WhenAll>
        Stopwatch stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        return new BulkOperationResponse<T>()
        {
            TotalTimeTaken = stopwatch.Elapsed,
            TotalRequestUnitsConsumed = tasks.Sum(task => task.Result.RequestUnitsConsumed),
            SuccessfulDocuments = tasks.Count(task => task.Result.IsSuccessful),
            Failures = tasks.Where(task => !task.Result.IsSuccessful).Select(task => (task.Result.Item, task.Result.CosmosException)).ToList()
        };
        // </WhenAll>
    }

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
                else if (record.FinishTime == null) continue;

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

        Console.WriteLine("Uploading runtime models.");
        Console.WriteLine($"Total count: {modelQueue.Count}");

        List<RuntimeModel> modelsToUpload = new List<RuntimeModel>();
        foreach (var model in modelQueue)
        {
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

            modelsToUpload.Add(model);
        }

        var runtimeRetries = new List<Task<OperationResponse<RuntimeModel>>>();
        int runtimeCreatedCount = 1;
        int runtimetotalJobCount = modelsToUpload.Count;

        // <BulkCreate>
        List<Task<OperationResponse<RuntimeModel>>> runtimeOperations = new List<Task<OperationResponse<RuntimeModel>>>(runtimetotalJobCount);
        foreach (RuntimeModel document in modelsToUpload)
        {
            runtimeOperations.Add(RuntimeContainer.CreateItemAsync<RuntimeModel>(document, new PartitionKey(document.BuildReasonString)).CaptureOperationResponse(document));
        }
        // </BulkCreate>

        Console.WriteLine($"Beginning bulk upload. Amount to upload: {runtimetotalJobCount}");

        BulkOperationResponse<RuntimeModel> runtimeBulkOperationResponse = await ExecuteTasksAsync(runtimeOperations);
        Console.WriteLine($"Bulk update operation finished in {runtimeBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {runtimeBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {runtimeBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {runtimeBulkOperationResponse.Failures.Count} documents");
        if (runtimeBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {runtimeBulkOperationResponse.Failures[0].Item1.Id} - {runtimeBulkOperationResponse.Failures[0].Item2}");


            foreach (var item in runtimeBulkOperationResponse.Failures)
            {
                runtimeRetries.Add(RuntimeContainer.CreateItemAsync<RuntimeModel>(item.Item1, new PartitionKey(item.Item1.BuildReasonString)).CaptureOperationResponse(item.Item1));
            }
        }

        runtimeCreatedCount += runtimeBulkOperationResponse.SuccessfulDocuments;

        while (runtimeRetries.Count > 0)
        {
            runtimeBulkOperationResponse = await ExecuteTasksAsync(runtimeOperations);
            Console.WriteLine($"Bulk update operation finished in {runtimeBulkOperationResponse.TotalTimeTaken}");
            Console.WriteLine($"Consumed {runtimeBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
            Console.WriteLine($"Created {runtimeBulkOperationResponse.SuccessfulDocuments} documents");
            Console.WriteLine($"Failed {runtimeBulkOperationResponse.Failures.Count} documents");

            runtimeRetries.Clear();
            
            if (runtimeBulkOperationResponse.Failures.Count > 0)
            {
                Console.WriteLine($"First failed sample document {runtimeBulkOperationResponse.Failures[0].Item1.Id} - {runtimeBulkOperationResponse.Failures[0].Item2}");


                foreach (var item in runtimeBulkOperationResponse.Failures)
                {
                    runtimeRetries.Add(RuntimeContainer.CreateItemAsync<RuntimeModel>(item.Item1, new PartitionKey(item.Item1.BuildReasonString)).CaptureOperationResponse(item.Item1));
                }
            }

            runtimeCreatedCount += runtimeBulkOperationResponse.SuccessfulDocuments;
        }

        // Job uploads

        var retries = new List<Task<OperationResponse<AzureDevOpsJobModel>>>();
        int jobsCreatedCount = 1;
        int totalJobCount = jobQueue.Count;

        int jobCount = 1;
        int sliceAmount = 100;

        // <BulkCreate>
        List<Task<OperationResponse<AzureDevOpsJobModel>>> operations = new List<Task<OperationResponse<AzureDevOpsJobModel>>>(totalJobCount);
        foreach (AzureDevOpsJobModel document in jobQueue)
        {
            if (jobCount++ % sliceAmount == 0)
            {
                Console.WriteLine($"[{jobsCreatedCount}:{totalJobCount}] Beginning bulk upload slice. Amount to upload: {sliceAmount}");
    
                var sliceBulkOperationResponse = await ExecuteTasksAsync(operations);
                Console.WriteLine($"Bulk update operation finished in {sliceBulkOperationResponse.TotalTimeTaken}");
                Console.WriteLine($"Consumed {sliceBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                Console.WriteLine($"Created {sliceBulkOperationResponse.SuccessfulDocuments} documents");
                Console.WriteLine($"Failed {sliceBulkOperationResponse.Failures.Count} documents");
                if (sliceBulkOperationResponse.Failures.Count > 0)
                {
                    Console.WriteLine($"First failed sample document {sliceBulkOperationResponse.Failures[0].Item1.Id} - {sliceBulkOperationResponse.Failures[0].Item2}");


                    foreach (var item in sliceBulkOperationResponse.Failures)
                    {
                        retries.Add(JobContainer.CreateItemAsync<AzureDevOpsJobModel>(item.Item1, new PartitionKey(item.Item1.Name)).CaptureOperationResponse(item.Item1));
                    }
                }

                jobsCreatedCount += sliceBulkOperationResponse.SuccessfulDocuments;

                operations.Clear();
            }

            operations.Add(JobContainer.CreateItemAsync<AzureDevOpsJobModel>(document, new PartitionKey(document.Name)).CaptureOperationResponse(document));
        }
        // </BulkCreate>

        Console.WriteLine($"[{jobsCreatedCount}:{totalJobCount}] Beginning bulk upload. Amount to upload: {operations.Count}");
    
        var bulkOperationResponse = await ExecuteTasksAsync(operations);
        Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {bulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
        if (bulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


            foreach (var item in bulkOperationResponse.Failures)
            {
                retries.Add(JobContainer.CreateItemAsync<AzureDevOpsJobModel>(item.Item1, new PartitionKey(item.Item1.Name)).CaptureOperationResponse(item.Item1));
            }
        }

        jobsCreatedCount += bulkOperationResponse.SuccessfulDocuments;

        while (retries.Count > 0)
        {
            bulkOperationResponse = await ExecuteTasksAsync(operations);
            Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
            Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
            Console.WriteLine($"Created {bulkOperationResponse.SuccessfulDocuments} documents");
            Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
            
            retries.Clear();

            if (bulkOperationResponse.Failures.Count > 0)
            {
                Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


                foreach (var item in bulkOperationResponse.Failures)
                {
                    retries.Add(JobContainer.CreateItemAsync<AzureDevOpsJobModel>(item.Item1, new PartitionKey(item.Item1.Name)).CaptureOperationResponse(item.Item1));
                }
            }

            jobsCreatedCount += bulkOperationResponse.SuccessfulDocuments;
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

        this.RuntimeContainer = await Db.CreateContainerIfNotExistsAsync(runtimeContainerProperties, throughput: 1000);
        this.JobContainer = await Db.CreateContainerIfNotExistsAsync(jobContainerProperties, throughput: 3000);
    }

}