////////////////////////////////////////////////////////////////////////////////
//
// Module: helix-io.cs
//
// Notes:
//
// Used to download as quickly and ingest data into cosmos as quickly as
// possible.
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
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
using ev27;
using DevOps.Util;

using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;

////////////////////////////////////////////////////////////////////////////////

internal class JobSubmission
{
    public long JobId { get; set; }
    public string JobName { get; set; }
    public string Source { get; set; }
    public string Type { get; set; }
    public int Creator { get; set; }
    public DateTime Queued { get; set; }
    public DateTime Started { get; set; }
    public DateTime Finished { get; set; }
    public int CountPassedfTests { get; set; }
    public int CountFailedTests { get; set; }
    public int CountSkippedTests { get; set; }
    public int CountItemsPassed { get; set; }
    public int CountItemsWarnings { get; set; }
    public int CountitemsBadExit { get; set; }
    public int CountItemsFailed { get; set; }
    public int CountItemsError { get; set; }
    public int CountItemsTimeout { get; set; }
    public int CountInitialItems { get; set; }
    public int CountTotalItems { get; set; }
    public string JobListUri { get; set; }
    public string QueueName { get; set; }
    public int CountItemsNotRun { get; set; }
    public int CountTestsPassedOnRetry { get; set; }
    public int CountItemsPassedOnRetry { get; set; }
    public string Repository { get; set; }
    public string Branch { get; set; }
    public string BuildNumber { get; set; }
}

internal class WorkItemDetails
{
    public string WorkItemFriendlyName { get; set; }
    public string LogUri { get; set; }
    public string JobName { get; set; }
    public string WorkItemName { get; set; }
    public int PassCount { get; set; }
    public int FailCount { get; set; }
    public int SkipCount { get; set; }
    public int WarnCount { get; set; }
    public DateTime Queued { get; set; }
    public DateTime Started { get; set; }
    public DateTime Finished { get; set; }
    public int ExitCode { get; set; }
    public string ConsoleUri { get; set; }
    public string MachineName { get; set; }
    public string WorkItemType { get; set; }
    public string TestResultsUri { get; set; }
    public int PassOnRetryCount { get; set; }
    public int Attempt { get; set; }

}

public class HelixIO
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public HelixIO(Container helixContainer, Container helixSubmissionContainer, Container XUnitTestContainer)
    {
        if (Uploader == null)
        {
            Action<HelixWorkItemModel> trimDoc = (HelixWorkItemModel document) => {
                long roughMaxSize = 1800000; // 1,800,000 bytes (1.8mb)

                if (document.Console != null)
                {
                    if (document.Console.Length > roughMaxSize)
                    {
                        document.Console = document.Console.Substring(0, (int)roughMaxSize);
                    }
                }

                Trace.Assert(document.ToString().Length < 2000000);
            };

            Action<HelixSubmissionModel> trimSubmissionDoc = (HelixSubmissionModel document) => {
                Trace.Assert(document.ToString().Length < 2000000);
            };

            Action<Test> trimTestDoc = (Test document) => {
                long roughMaxSize = 1800000; // 1,800,000 bytes (1.8mb)

                if (document.Console != null)
                {
                    if (document.Console.Length > roughMaxSize)
                    {
                        document.Console = document.Console.Substring(0, (int)roughMaxSize);
                    }
                }

                Trace.Assert(document.ToString().Length < 2000000);
            };

            Queue = new Queue<HelixWorkItemModel>();
            Uploader = new CosmosUpload<HelixWorkItemModel>("[Helix Work Item Model Upload]", helixContainer, Queue, (HelixWorkItemModel document) => { return document.Name; }, trimDoc);

            Uploader.DocCap = 100;

            SubmissionQueue = new Queue<HelixSubmissionModel>();
            SubmissionUploader = new CosmosUpload<HelixSubmissionModel>("[Helix Submision Model Upload]", helixSubmissionContainer, SubmissionQueue, (HelixSubmissionModel document) => { return document.Name; }, trimSubmissionDoc);

            TestQueue = new Queue<Test>();
            TestUploader = new CosmosUpload<Test>("[XUnitTest Upload]", XUnitTestContainer, TestQueue, (Test document) => { return document.Name; }, trimTestDoc);

            TestUploader.DocCap = 300;
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    private static Queue<HelixWorkItemModel> Queue = null;
    private static CosmosUpload<HelixWorkItemModel> Uploader = null;
    private static Queue<HelixSubmissionModel> SubmissionQueue = null;
    private static CosmosUpload<HelixSubmissionModel> SubmissionUploader = null;
    private static Queue<Test> TestQueue = null;
    private static CosmosUpload<Test> TestUploader = null;

    private static object HelixLock = new object();


    private List<HelixWorkItemModel> WorkItems = new List<HelixWorkItemModel>();

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    // job, step id, step name
    public async Task IngestData(List<Tuple<string, AzureDevOpsStepModel, AzureDevOpsJobModel>> helixJobs)
    {
        Trace.Assert(helixJobs.Count > 0);
        List<HelixSubmissionModel> helixSubmissions = new List<HelixSubmissionModel>();
        var allWorkItems = new List<Tuple<HelixWorkItemDetail, HelixSubmissionModel, AzureDevOpsJobModel>>();
        object workItemLock = new object();

        string token = Environment.GetEnvironmentVariable("kustoKey");
        string clientId = Environment.GetEnvironmentVariable("clientId");
        string authority = Environment.GetEnvironmentVariable("authorityId");
        string connectionString = $"Data Source=https://engsrvprod.kusto.windows.net:443;AAD Federated Security=True;Application Client Id={clientId};Application Key={token};Authority ID={authority}";
        
        var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider(connectionString);
        var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

        string buildId = helixJobs[0].Item3.PipelineId;

        Dictionary<string, Tuple<AzureDevOpsStepModel, AzureDevOpsJobModel>> jobsWithSubmissions = new Dictionary<string, Tuple<AzureDevOpsStepModel, AzureDevOpsJobModel>>();
        foreach (var helixJob in helixJobs)
        {
            Debug.Assert(!jobsWithSubmissions.ContainsKey(helixJob.Item1));
            jobsWithSubmissions[helixJob.Item1] = new Tuple<AzureDevOpsStepModel, AzureDevOpsJobModel>(helixJob.Item2, helixJob.Item3);
        }

        var query = @"Jobs
| extend Properties=parse_json(Properties)
| extend BuildNumber=tostring(Properties.BuildNumber), Project=tostring(Properties.Project)";
        query += $"| where BuildNumber == '{buildId}' and Project == 'public'";

        Dictionary<string, JobSubmission> submissions = new Dictionary<string, JobSubmission>();

        using (var reader = client.ExecuteQuery("engineeringdata", query, clientRequestProperties))
        {
            // Determine count of records
            while(reader.Read())
            {
                JobSubmission submission = new JobSubmission();

                submission.JobId = reader.GetInt64(0);
                submission.JobName = reader.GetString(1);
                submission.Source = reader.GetString(2);
                submission.Type = reader.GetString(3);
                submission.Creator = reader.GetInt32(5);
                submission.Queued = reader.GetDateTime(9);
                submission.Started = reader.GetDateTime(10);
                submission.Finished = reader.GetDateTime(11);
                submission.CountPassedfTests = reader.GetInt32(12);
                submission.CountFailedTests = reader.GetInt32(13);
                submission.CountSkippedTests = reader.GetInt32(14);

                submission.CountItemsPassed = reader.GetInt32(15);
                submission.CountItemsWarnings = reader.GetInt32(16);
                submission.CountitemsBadExit = reader.GetInt32(17);
                submission.CountItemsFailed = reader.GetInt32(18);
                submission.CountItemsError = reader.GetInt32(19);
                submission.CountItemsTimeout = reader.GetInt32(20);
                
                submission.CountInitialItems = reader.GetInt32(21);
                submission.CountTotalItems = reader.GetInt32(22);

                submission.JobListUri = reader.GetString(23);
                submission.QueueName = reader.GetString(25);

                submission.CountItemsNotRun = reader.GetInt32(26);
                submission.CountTestsPassedOnRetry = reader.GetInt32(27);
                submission.CountItemsPassedOnRetry = reader.GetInt32(28);

                submission.Repository = reader.GetString(34);
                submission.Branch = reader.GetString(35);
                submission.BuildNumber = reader.GetString(36);

                submissions.Add(submission.JobName, submission);
            }
        }

        foreach (var submissionKeyValue in submissions)
        {
            HelixSubmissionModel model = new HelixSubmissionModel();

            if (!jobsWithSubmissions.ContainsKey(submissionKeyValue.Key))
            {
                // This is a retry job
                continue;
            }

            var helixJob = jobsWithSubmissions[submissionKeyValue.Key];

            JobSubmission submission = submissionKeyValue.Value;

            model.JobId = helixJob.Item2.JobGuid;
            model.JobName = helixJob.Item2.Name;

            model.Passed = submission.CountTotalItems == submission.CountItemsPassed;
            model.Queues = new List<string>() { submission.QueueName };
            model.RuntimePipelineId = buildId;
            model.Source = submission.Branch;
            model.Start = submission.Started;

            model.StepId = helixJob.Item1.Id;

            model.End = submission.Finished;
            model.ElapsedTime = (model.End - model.Start).Milliseconds;

            model.Id = Guid.NewGuid().ToString();
            model.Type = submission.Type;

            model.WorkItemCount = submission.CountInitialItems;

            model.HelixJobName = submission.JobName;

            helixSubmissions.Add(model);
        }

        Dictionary<string, HelixSubmissionModel> submissionsById = new Dictionary<string, HelixSubmissionModel>();

        foreach (var submissionModel in helixSubmissions)
        {
            submissionsById.Add(submissionModel.HelixJobName, submissionModel);
        }

        query = @"Jobs
| extend Properties=parse_json(Properties)
| extend BuildNumber=tostring(Properties.BuildNumber), Project=tostring(Properties.Project)";
        query += $"| where BuildNumber == '{buildId}' and Project == 'public'";
        query += @"| project JobId
| join kind=inner(WorkItems | project JobId, JobName, WorkItemId, Name, PassCount, FailCount, SkipCount, WarnCount, Queued, Started, Finished, ExitCode, ConsoleUri, MachineName, WorkItemType, Uri, PassOnRetryCount, Attempt, QueueName) on JobId
| project-away JobId1
| extend WorkItemName=Name
| project-away Name
| join (Logs | where Module == 'run_client.py' | project WorkItemFriendlyName, LogUri, WorkItemName, JobId) on WorkItemName
| project-away WorkItemName1, JobId, JobId1, WorkItemId, QueueName";

        Dictionary<string, WorkItemDetails> workItems = new Dictionary<string, WorkItemDetails>();

        // Now go through all workitems
        using (var reader = await client.ExecuteQueryAsync("engineeringdata", query, clientRequestProperties))
        {
            // Determine count of records
            while(reader.Read())
            {
                WorkItemDetails details = new WorkItemDetails();

                details.JobName = reader.GetString(0);
                details.PassCount = reader.GetInt32(1);
                details.FailCount = reader.GetInt32(2);
                details.SkipCount = reader.GetInt32(3);
                details.WarnCount = reader.GetInt32(4);
                details.Queued = reader.GetDateTime(5);
                details.Started = reader.GetDateTime(6);
                details.Finished = reader.GetDateTime(7);
                details.ExitCode = reader.GetInt32(8);
                details.ConsoleUri = reader.GetString(9);
                details.MachineName = reader.GetString(10);
                details.WorkItemType = reader.GetString(11);
                details.TestResultsUri = reader.GetString(12);
                details.PassOnRetryCount = reader.GetInt32(13);
                details.Attempt = reader.GetInt32(14);
                details.WorkItemName = reader.GetString(15);
                details.WorkItemFriendlyName = reader.GetString(16);
                details.LogUri = reader.GetString(17);

                Debug.Assert(!workItems.ContainsKey(details.WorkItemName));

                workItems.Add(details.WorkItemName, details);
            }
        }

        List<Task> tasks = new List<Task>();

        int currentItem = 1;
        int totalItems = workItems.Count;
        int limit = 500;
        DateTime helixWorkitemDownloadStartTime = DateTime.Now;
        DateTime limitStart = DateTime.Now;

        foreach(var workItemKeyValue in workItems)
        {
            if (!submissionsById.ContainsKey(workItemKeyValue.Value.JobName))
            {
                ++currentItem;
                continue;
            }

            HelixSubmissionModel model = submissionsById[workItemKeyValue.Value.JobName];

            Debug.Assert(jobsWithSubmissions.ContainsKey(model.HelixJobName));
            if (!jobsWithSubmissions.ContainsKey(model.HelixJobName))
            {
                ++currentItem;
                continue;
            }

            var jobModel = jobsWithSubmissions[model.HelixJobName];
            
            Console.WriteLine($"[{currentItem++}:{totalItems}]: Started.");
            tasks.Add(PopulateHelixWorkItem(buildId, workItemKeyValue.Value, model, jobModel.Item2));

            if (tasks.Count == limit)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
                
                DateTime limitEnd = DateTime.Now;
                double elapsedLimitSeconds = (limitEnd - limitStart).TotalSeconds;

                Console.WriteLine($"[Helix WorkItem] -- {limit} downloaded in {elapsedLimitSeconds}s");
            }
        }

        await Task.WhenAll(tasks);
        tasks.Clear();

        DateTime helixWorkitemDownloadEndTime = DateTime.Now;
        double elapsedHelixWorkItemDownloadtime = (helixWorkitemDownloadEndTime - helixWorkitemDownloadStartTime).TotalMinutes;

        Console.WriteLine($"Total time to downlaod {totalItems}: {elapsedHelixWorkItemDownloadtime}m");

        foreach (var submission in helixSubmissions)
        {
            SubmissionQueue.Enqueue(submission);
        }

        foreach (var workItemModel in WorkItems)
        {
            SubmitToUpload(workItemModel);
        }

        long testCount = TestQueue.Count;

        DateTime testBegin = DateTime.Now;
        // Upload tests.
        await TestUploader.Finish();
        DateTime testEnd = DateTime.Now;

        var elapsedTestUploadTime = (testEnd - testBegin).TotalMinutes;

        Console.WriteLine($"Uploaded {testCount} in {elapsedTestUploadTime}m");

        // Upload submissions.
        await SubmissionUploader.Finish();

        // Upload workitems
        await Uploader.Finish();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////

    private async Task PopulateHelixWorkItem(string buildId, WorkItemDetails detail, HelixSubmissionModel model, AzureDevOpsJobModel jobModel)
    {
        DateTime startHelixWorkitem = DateTime.Now;
        var modelToAdd = new HelixWorkItemModel();

        HelixWorkItemModel workItemModel = new HelixWorkItemModel();
        workItemModel.ExitCode = detail.ExitCode;
        workItemModel.MachineName = detail.MachineName;
        workItemModel.Name = detail.WorkItemFriendlyName;
        workItemModel.JobId = detail.JobName;
        workItemModel.RuntimePipelineId = buildId;

        workItemModel.Id = Guid.NewGuid().ToString();

        string logUri = detail.LogUri;
        string testXmlUri = detail.TestResultsUri;

        if (testXmlUri != null && testXmlUri != "")
        {
            string xmlContents = null;
            try
            {
                xmlContents = await Shared.GetAsync(testXmlUri);
            }
            catch (Exception e)
            {
                return;
            }

            try
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(xmlContents);

                int totalRunTests = 0;
                int passedTests = 0;
                int failedTests = 0;
                XmlNodeList collections = doc.GetElementsByTagName("collection");

                foreach (XmlNode collection in collections)
                {
                    int amountOfFailedTests = 0;
                    
                    var passedStr = collection.Attributes["passed"].Value;
                    var failedStr = collection.Attributes["failed"].Value;

                    passedTests += int.Parse(passedStr);
                    amountOfFailedTests = int.Parse(failedStr);

                    failedTests += amountOfFailedTests;

                    foreach (XmlNode test in collection.SelectNodes("test"))
                    {
                        string console = null;
                        if (test.ChildNodes.Count > 0)
                        {
                            console = test.ChildNodes[0].InnerText;
                        }

                        string testName = test.Attributes["name"].Value;
                        bool passed = test.Attributes["result"].Value == "Pass" ? true : false;
                        double timeRun = Double.Parse(test.Attributes["time"].Value);
                        
                        Test testModel = new Test();

                        if (passed)
                        {
                            testModel.Console = null;
                        }
                        else
                        {
                            testModel.Console = console;
                        }

                        testModel.ElapsedTime = timeRun;
                        testModel.Name = testName;
                        testModel.Passed = passed;

                        testModel.Id = Guid.NewGuid().ToString();
                        testModel.HelixWorkItemId = workItemModel.Id;

                        SubmitTestToUpload(testModel);
                    }
                }

                totalRunTests = passedTests + failedTests;

                workItemModel.TotalRunTests = totalRunTests;
                workItemModel.PassedTests = passedTests;
                workItemModel.FailedTests = failedTests;

                modelToAdd.TotalRunTests = workItemModel.TotalRunTests;
                modelToAdd.PassedTests = workItemModel.PassedTests;
                modelToAdd.FailedTests = workItemModel.FailedTests;
            }
            catch (Exception e)
            {
                // No test stats for this workitem.
            }
        }

        if (logUri != null)
        {
            try
            {
                Trace.Assert(logUri != null);

                string helixRunnerLog = null;
                bool continueDueToException = false;
                try
                {
                    helixRunnerLog = await Shared.GetAsync(logUri);
                }
                catch (Exception e)
                {
                    continueDueToException = true;
                }

                if (continueDueToException)
                {
                    return;
                }

                string delim = "\t";
                var zSplit = helixRunnerLog.Split('Z');
                if (zSplit.Length == 1)
                {
                    delim = ": ";
                }
                else
                {
                    if (zSplit[1][0] != '\t')
                    {
                        Trace.Assert(!helixRunnerLog.Contains("_dump_file_upload"));
                        delim = ": ";
                    }
                }

                string setupBeginStr = helixRunnerLog.Split(delim)[0];

                if (helixRunnerLog.Contains("dockerhelper"))
                {
                    string splitString = helixRunnerLog.Split("write_commands_to_file")[0];
                    var splitStringLines = splitString.Split('\n');

                    string setupEndStr = splitStringLines[splitStringLines.Length - 1].Split(delim)[0];

                    setupBeginStr = Regex.Replace(setupBeginStr, @"\s", "0");
                    setupEndStr = Regex.Replace(setupEndStr, @"\s", "0");

                    if (delim == ": ")
                    {
                        setupBeginStr = Regex.Replace(setupBeginStr, @",", ".");
                        setupEndStr = Regex.Replace(setupEndStr, @",", ".");

                        setupBeginStr += "Z";
                        setupEndStr += "Z";

                        setupBeginStr = Regex.Replace(setupBeginStr, @"\s+", "T");
                        setupEndStr = Regex.Replace(setupEndStr, @"\s+", "T");
                    }
                    else
                    {
                        setupBeginStr = Regex.Replace(setupBeginStr, @"\s", "0");
                        setupEndStr = Regex.Replace(setupEndStr, @"\s", "0");
                    }

                    try
                    {
                        DateTime setupStartTime = DateTime.Parse(setupBeginStr);
                        DateTime setupEndTime = DateTime.Parse(setupEndStr);
                        
                        workItemModel.HelixWorkItemSetupBegin = setupStartTime;
                        workItemModel.HelixWorkItemSetupEnd = setupEndTime;
                        
                        workItemModel.RunBegin = setupEndTime;
                    }
                    catch (Exception e)
                    {
                        string path = @"strange_uris.txt";
                        if (!File.Exists(path))
                        {
                            // Create a file to write to.
                            using (StreamWriter sw = File.CreateText(path))
                            {
                                sw.WriteLine($"{logUri}");
                            }
                        }
                        else
                        {
                            using (StreamWriter sw = File.AppendText(path))
                            {
                                sw.WriteLine($"{logUri}");
                            }
                        }

                        return;
                    }

                }
                else
                {
                    string splitString = helixRunnerLog.Split("_execute_command")[0];
                    var splitStringLines = splitString.Split("\n");

                    string setupEndStr = splitStringLines[splitStringLines.Length - 1].Split(delim)[0];

                    if (delim == ": ")
                    {
                        setupBeginStr = Regex.Replace(setupBeginStr, @",", ".");
                        setupEndStr = Regex.Replace(setupEndStr, @",", ".");

                        setupBeginStr += "Z";
                        setupEndStr += "Z";

                        setupBeginStr = Regex.Replace(setupBeginStr, @"\s+", "T");
                        setupEndStr = Regex.Replace(setupEndStr, @"\s+", "T");
                    }
                    else
                    {
                        setupBeginStr = Regex.Replace(setupBeginStr, @"\s", "0");
                        setupEndStr = Regex.Replace(setupEndStr, @"\s", "0");
                    }

                    try
                    {
                        DateTime setupStartTime = DateTime.Parse(setupBeginStr);
                        DateTime setupEndTime = DateTime.Parse(setupEndStr);

                        workItemModel.HelixWorkItemSetupBegin = setupStartTime;
                        workItemModel.HelixWorkItemSetupEnd = setupEndTime;

                        workItemModel.RunBegin = setupEndTime;
                    }
                    catch (Exception e)
                    {
                        string path = @"strange_uris.txt";
                        if (!File.Exists(path))
                        {
                            // Create a file to write to.
                            using (StreamWriter sw = File.CreateText(path))
                            {
                                sw.WriteLine($"{logUri}");
                            }
                        }
                        else
                        {
                            using (StreamWriter sw = File.AppendText(path))
                            {
                                sw.WriteLine($"{logUri}");
                            }
                        }

                        return;
                    }
                }

                string endDelim = delim == ": " ? "_execute_command: Finished" : "_dump_file_upload";
                string runtimeSplitStr = helixRunnerLog.Split(endDelim)[0];
                var runtimeSplitStrLines = runtimeSplitStr.Split('\n');

                string runtimeEndStr = runtimeSplitStrLines[runtimeSplitStrLines.Length - 1].Split(delim)[0];

                if (delim == ": ")
                {
                    runtimeEndStr = Regex.Replace(runtimeEndStr, @",", ".");

                    runtimeEndStr += "Z";

                    runtimeEndStr = Regex.Replace(runtimeEndStr, @"\s+", "T");
                }
                else
                {
                    runtimeEndStr = Regex.Replace(runtimeEndStr, @"\s", "0");
                }

                DateTime runtimeEndTime = DateTime.Parse(runtimeEndStr);

                workItemModel.RunEnd = runtimeEndTime;

                workItemModel.ElapsedSetupTime = (workItemModel.HelixWorkItemSetupEnd - workItemModel.HelixWorkItemSetupBegin).TotalMilliseconds;
                workItemModel.ElapsedRunTime = (workItemModel.RunEnd - workItemModel.RunBegin).TotalMilliseconds;

                Debug.Assert(workItemModel.ElapsedRunTime > 0);
                Debug.Assert(workItemModel.ElapsedSetupTime > 0);

                if (workItemModel.ElapsedRunTime < 0 || workItemModel.ElapsedSetupTime < 0)
                {
                    string path = @"strange_uris.txt";
                    if (!File.Exists(path))
                    {
                        // Create a file to write to.
                        using (StreamWriter sw = File.CreateText(path))
                        {
                            sw.WriteLine($"{logUri}");
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = File.AppendText(path))
                        {
                            sw.WriteLine($"{logUri}");
                        }
                    }

                    return;
                }

                if (workItemModel.ExitCode != 0)
                {
                    workItemModel.Console = null;
                    
                    try
                    {
                        workItemModel.Console = await Shared.GetAsync(detail.ConsoleUri);
                    }
                    catch(Exception e)
                    {
                        // do nothing.
                    }
                }

                workItemModel.HelixSubmissionId = model.Id;
                workItemModel.StepId = model.StepId;

                workItemModel.JobName = jobModel.Name;

                modelToAdd.Id = workItemModel.Id;
                modelToAdd.ElapsedRunTime = workItemModel.ElapsedRunTime;
                modelToAdd.ElapsedSetupTime = workItemModel.ElapsedSetupTime;
                modelToAdd.HelixWorkItemSetupBegin = workItemModel.HelixWorkItemSetupBegin;
                modelToAdd.HelixWorkItemSetupEnd = workItemModel.HelixWorkItemSetupEnd;
                modelToAdd.MachineName = workItemModel.MachineName;
                modelToAdd.Name = workItemModel.Name;
                modelToAdd.RunBegin = workItemModel.RunBegin;
                modelToAdd.RunEnd = workItemModel.RunEnd;
                modelToAdd.JobName = workItemModel.JobName;
                modelToAdd.JobId = workItemModel.JobId;
                modelToAdd.RuntimePipelineId = workItemModel.RuntimePipelineId;
                modelToAdd.StepId = workItemModel.StepId;
                
                modelToAdd.HelixSubmissionId = model.Id;
            }
            catch(Exception e)
            {
                string path = @"strange_uris.txt";
                if (!File.Exists(path))
                {
                    // Create a file to write to.
                    using (StreamWriter sw = File.CreateText(path))
                    {
                        sw.WriteLine($"{logUri}");
                    }
                }
                else
                {
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine($"{logUri}");
                    }
                }

                return;
            }
        }

        DateTime endHelixWorkItem = DateTime.Now;
        double elapsedTime = (endHelixWorkItem - startHelixWorkitem).TotalMilliseconds;

        WorkItems.Add(modelToAdd);

        Console.WriteLine($"[Helix Workitem] -- [{modelToAdd.Name}]:  in {elapsedTime} ms");
    }

    private void SubmitToUploadSubmission(HelixSubmissionModel model)
    {
        Trace.Assert(model != null);
        lock(HelixLock)
        {
            SubmissionQueue.Enqueue(model);
        }
    }

    private void SubmitToUpload(HelixWorkItemModel model)
    {
        Trace.Assert(model != null);
        lock(HelixLock)
        {
            long roughMaxSize = 1800000; // 1,800,000 bytes (1.8mb)

            if (model.Console != null)
            {
                if (model.Console.Length > roughMaxSize)
                {
                    model.Console = model.Console.Substring(0, (int)roughMaxSize);
                }
            }

            Trace.Assert(model.ToString().Length < 2000000);
            Queue.Enqueue(model);
        }
    }

    private void SubmitTestToUpload(Test model)
    {
        lock(HelixLock)
        {
            long roughMaxSize = 1800000; // 1,800,000 bytes (1.8mb)

            if (model.Console != null)
            {
                if (model.Console.Length > roughMaxSize)
                {
                    model.Console = model.Console.Substring(0, (int)roughMaxSize);
                }
            }

            Trace.Assert(model.ToString().Length < 2000000);
            TestQueue.Enqueue(model);
        }
    }

}
