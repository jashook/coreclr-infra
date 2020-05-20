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

using ev27;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class HelixWorkItemDetail
{
    public string DetailsUrl { get; set; }
    public string Job { get; set; }
    public string Name { get; set; }
    public string State { get; set; }

}

public class HelixWorkItem
{
    public string FailureReason { get; set; }
    public string Id { get; set; }
    public string MachineName { get; set; }
    public int ExitCode { get; set; }
    public string ConsoleOutputUri { get; set; }
    public List<Dictionary<string, string>> Errors { get; set; }
    public List<Dictionary<string, string>> Warnings { get; set; }
    public List<Dictionary<string, string>>  Logs { get; set; }
    public List<Dictionary<string, string>>  Files { get; set; }
    public string Job { get; set; }
    public string Name { get; set; }
}

public class HelixWorkItemSummary
{
    public string FailureReason { get; set; }
    public string QueueId { get; set; }
    public string DetailsUrl { get; set; }
    public string Name { get; set; }
    public string Created { get; set; }
    public string Finished { get; set; }
    public string InitalWorkItemCount { get; set; }
    public string Source { get; set; }
    public string Type { get; set; }
    public Dictionary<string, string> Properties { get; set; }
}

public class AzureDevopsTracking
{
    ////////////////////////////////////////////////////////////////////////////
    // Member variables.
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // Private member variables.
    ////////////////////////////////////////////////////////////////////////////

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
    private Container HelixContainer;
    private Container HelixSubmissionContainer;
    private Container XUnitTestContainer;

    private static readonly string DatabaseName = "coreclr-infra";
    private static readonly string RuntimeContainerName = "runtime-pipelines";
    private static readonly string JobContainerName = "runtime-jobs";
    private static readonly string HelixContainerName = "helix-workitems";
    private static readonly string HelixSubmissionContainerName = "helix-submissions";
    private static readonly string XUnitTestContainerName = "xunit-tests";

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

    public async Task Recalculate(bool recalculatePipelineElapsedTime=false,
                                  bool redownloadLogs=false,
                                  bool force=false,
                                  DateTime? begin=null,
                                  DateTime? end=null)
    {
        if (recalculatePipelineElapsedTime)
        {
            int pipelineUpdateCount = 0;
            int pipelineFailureCount = 0;
            try
            {
                var queryable = RuntimeContainer.GetItemLinqQueryable<RuntimeModel>();

                FeedIterator<RuntimeModel> iterator = null;
                
                if (begin != null && end != null)
                {
                    var query = queryable.Where(item => item.DateStart > begin && item.DateEnd < end);
                    iterator = query.ToFeedIterator();
                }
                else if (begin != null)
                {
                    var query = queryable.Where(item => item.DateStart > begin);
                    iterator = query.ToFeedIterator();
                }
                else if (end != null)
                {
                    var query = queryable.Where(item => item.DateEnd < end);
                    iterator = query.ToFeedIterator();
                }
                else
                {
                    iterator = queryable.ToFeedIterator();
                }

                List<Task<OperationResponse<RuntimeModel>>> retries = new List<Task<OperationResponse<RuntimeModel>>>();

                while (iterator.HasMoreResults)
                {
                    var cosmosResult = await iterator.ReadNextAsync();

                    IEnumerable<RuntimeModel> items = cosmosResult.Take(cosmosResult.Resource.Count());

                    // <BulkDelete>
                    List<Task<OperationResponse<RuntimeModel>>> operations = new List<Task<OperationResponse<RuntimeModel>>>(items.Count());
                    foreach (RuntimeModel document in items)
                    {
                        if (recalculatePipelineElapsedTime)
                        {
                            document.ElapsedTime = (long)((document.DateEnd - document.DateStart).TotalSeconds);
                        }
                        
                        operations.Add(RuntimeContainer.ReplaceItemAsync<RuntimeModel>(document, document.Id, new PartitionKey(document.BuildReasonString)).CaptureOperationResponse(document));
                    }
                    // </BulkDelete>

                    BulkOperationResponse<RuntimeModel> bulkOperationResponse = await Shared.ExecuteTasksAsync(operations);
                    Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
                    Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                    Console.WriteLine($"Replaced {bulkOperationResponse.SuccessfulDocuments} documents");
                    Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
                    if (bulkOperationResponse.Failures.Count > 0)
                    {
                        Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


                        foreach (var item in bulkOperationResponse.Failures)
                        {
                            retries.Add(JobContainer.ReplaceItemAsync<RuntimeModel>(item.Item1, item.Item1.Id, new PartitionKey(item.Item1.BuildReasonString)).CaptureOperationResponse(item.Item1));
                        }
                    }

                    pipelineUpdateCount += bulkOperationResponse.SuccessfulDocuments;
                }

                while (retries.Count > 0)
                {
                    BulkOperationResponse<RuntimeModel> bulkOperationResponse = await Shared.ExecuteTasksAsync(retries);
                    Console.WriteLine($"Bulk update operation finished in {bulkOperationResponse.TotalTimeTaken}");
                    Console.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
                    Console.WriteLine($"Replaced {bulkOperationResponse.SuccessfulDocuments} documents");
                    Console.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
                    if (bulkOperationResponse.Failures.Count > 0)
                    {
                        Console.WriteLine($"First failed sample document {bulkOperationResponse.Failures[0].Item1.Id} - {bulkOperationResponse.Failures[0].Item2}");


                        foreach (var item in bulkOperationResponse.Failures)
                        {
                            retries.Add(JobContainer.ReplaceItemAsync<RuntimeModel>(item.Item1, item.Item1.Id, new PartitionKey(item.Item1.BuildReasonString)).CaptureOperationResponse(item.Item1));
                        }
                    }

                    pipelineUpdateCount += bulkOperationResponse.SuccessfulDocuments;
                }
            }
            catch (Exception e)
            {
                Console.Write(e);
                ++pipelineFailureCount;
            }
        }
        else if (redownloadLogs)
        {
            var queryable = JobContainer.GetItemLinqQueryable<AzureDevOpsJobModel>();
            FeedIterator<AzureDevOpsJobModel> iterator = null;
            
            if (begin != null && end != null)
            {
                var query = queryable.Where(item => item.Result == TaskResult.Failed && item.DateStart > begin && item.DateEnd < end);
                iterator = query.ToFeedIterator();
            }
            else if (begin != null)
            {
                var query = queryable.Where(item => item.Result == TaskResult.Failed && item.DateStart > begin);
                iterator = query.ToFeedIterator();
            }
            else if (end != null)
            {
                var query = queryable.Where(item => item.Result == TaskResult.Failed &&  item.DateEnd < end);
                iterator = query.ToFeedIterator();
            }
            else
            {
                iterator = queryable.ToFeedIterator();
            }

            JobIO io = new JobIO(Db);
            await io.ReUploadData(iterator, force);
        }
    }

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

                BulkOperationResponse<AzureDevOpsJobModel> bulkOperationResponse = await Shared.ExecuteTasksAsync(operations);
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
                BulkOperationResponse<AzureDevOpsJobModel> bulkOperationResponse = await Shared.ExecuteTasksAsync(retries);
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
        int limit = 1;

        var lastRun = await GetLastRunFromDb();

        List<Build> builds = await Server.ListBuildsAsync("public", new int[] {
            686
        });

        List<RuntimeModel> runs = null;
        
        bool failed = false;
        do
        {
            try
            {
                runs = await GetRunsSince(lastRun, builds, limit);
                failed = false;
            }
            catch (Exception e)
            {
                failed = true;
            }
        } while (failed);

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

            do
            {
                try
                {
                    runs = await GetRunsSince(lastRun, builds, limit);
                    failed = false;
                }
                catch (Exception e)
                {
                    failed = true;
                }
            } while (failed);
        }

        Trace.Assert(runs.Count == 0);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Private types
    ////////////////////////////////////////////////////////////////////////////

    

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
        Trace.Assert(builds != null);

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
        List<Task> tasks = new List<Task>();
        object modelLock = new object();
        foreach (var build in buildRecords)
        {
            tasks.Add(Task.Run(() => { AddToModels(modelLock, build, index++, total, models).Wait(); }));

            if (tasks.Count > limit)
            {
                break;
            }
        }

        await Task.WhenAll(tasks);
        return models;
    }

    private async Task AddToModels(object modelLock, Build build, int index, int total, List<RuntimeModel> models)
    {
        Console.WriteLine($"[{index}:{total}]");
        RuntimeModel model = new RuntimeModel();
        if (build.FinishTime == null)
        {
            return;
        }

        model.BuildNumber = build.BuildNumber;
        model.DateEnd = DateTime.Parse(build.FinishTime);
        model.DateStart = DateTime.Parse(build.StartTime);
        model.ElapsedTime = (long)((model.DateEnd - model.DateStart).TotalSeconds);
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
            return;
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
                    step.Console = await Shared.GetAsync(step.ConsoleUri, retryCount: 1);
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
            Trace.Assert(records.ContainsKey(kv.Key));
            var record = records[kv.Key];

            if (record.StartTime == null) continue;
            else if (record.FinishTime == null) continue;

            jobModel.DateEnd = DateTime.Parse(record.FinishTime);
            jobModel.DateStart = DateTime.Parse(record.StartTime);
            jobModel.ElapsedTime = (jobModel.DateEnd - jobModel.DateStart).TotalMinutes;
            
            jobModel.JobGuid = record.Id;

            Trace.Assert(record.Id == kv.Key);

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
                    else if (step.Name.ToLower().Contains("runtimetests"))
                    {
                        // Do nothing/////
                    }
                    else
                    {
                        // Unreached
                        Trace.Assert(false);
                    }
                }
            }
        }

        lock(modelLock)
        {
            models.Add(model);
        }
    }

    private async Task UploadRuns(List<RuntimeModel> models)
    {
        Queue<AzureDevOpsJobModel> jobQueue = new Queue<AzureDevOpsJobModel>();

        Queue<RuntimeModel> queue = new Queue<RuntimeModel>();
        CosmosUpload<RuntimeModel> uploader = new CosmosUpload<RuntimeModel>("[Runtime Model Upload]", RuntimeContainer, queue, (RuntimeModel doc) => { return doc.BuildReasonString; }, (RuntimeModel doc) => { });

        foreach (var model in models)
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

            queue.Enqueue(model);
        }

        Console.WriteLine("Uploading runtime models.");
        Console.WriteLine($"Total count: {models.Count}");

        // Job uploads

        JobIO io = new JobIO(Db);

        Queue<AzureDevOpsJobModel> jobs = new Queue<AzureDevOpsJobModel>();
        foreach (var item in jobQueue)
        {
            jobs.Enqueue(item);
        }
        jobQueue = null;

        await io.UploadData(jobs);
        await uploader.Finish();
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
            try
            {
                await Db.GetContainer(RuntimeContainerName).DeleteContainerAsync();
            }
            catch (Exception e)
            {

            }
            
            try
            {
                await Db.GetContainer(JobContainerName).DeleteContainerAsync();
            }
            catch (Exception e)
            {

            }

            try
            {
                await Db.GetContainer(HelixContainerName).DeleteContainerAsync();
            }
            catch (Exception e)
            {

            }

            try
            {
                await Db.GetContainer(HelixSubmissionContainerName).DeleteContainerAsync();
            }
            catch (Exception e)
            {

            }

            try
            {
                await Db.GetContainer(XUnitTestContainerName).DeleteContainerAsync();
            }
            catch (Exception e)
            {

            }
        }

        ContainerProperties runtimeContainerProperties = new ContainerProperties(RuntimeContainerName, partitionKeyPath: "/BuildReasonString");
        ContainerProperties jobContainerProperties = new ContainerProperties(JobContainerName, partitionKeyPath: "/Name");
        ContainerProperties helixContainerProperties = new ContainerProperties(HelixContainerName, partitionKeyPath: "/Name");
        ContainerProperties helixSubmissionContainerProperties = new ContainerProperties(HelixSubmissionContainerName, partitionKeyPath: "/Name");
        ContainerProperties xunitContainerProperties = new ContainerProperties(XUnitTestContainerName, partitionKeyPath: "/Name");

        this.RuntimeContainer = await Db.CreateContainerIfNotExistsAsync(runtimeContainerProperties, throughput: 1000);
        this.JobContainer = await Db.CreateContainerIfNotExistsAsync(jobContainerProperties, throughput: 3000);
        this.HelixContainer = await Db.CreateContainerIfNotExistsAsync(helixContainerProperties, throughput: 1500);
        this.HelixSubmissionContainer = await Db.CreateContainerIfNotExistsAsync(helixSubmissionContainerProperties, throughput: 500);
        this.XUnitTestContainer = await Db.CreateContainerIfNotExistsAsync(xunitContainerProperties, throughput: 1500);
    }

}