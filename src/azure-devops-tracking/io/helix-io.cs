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

////////////////////////////////////////////////////////////////////////////////

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

        List<Task> tasks = new List<Task>();
        foreach (var jobTuple in helixJobs)
        {
            tasks.Add(DownloadSubmission(jobTuple, helixSubmissions, allWorkItems, workItemLock));
        }

        DateTime helixSubmissionsStartTime = DateTime.Now;
        Console.WriteLine("Starting download helix submissions.");
        await Task.WhenAll(tasks);
        tasks.Clear();

        foreach (var submission in helixSubmissions)
        {
            SubmissionQueue.Enqueue(submission);
        }

        DateTime helixSubmissionsEndTime = DateTime.Now;
        double elapsedHelixSubmissionDownloadtime = (helixSubmissionsEndTime - helixSubmissionsStartTime).TotalSeconds;

        Console.WriteLine($"Downloaded {helixSubmissions.Count} helix submissions in {elapsedHelixSubmissionDownloadtime}s");

        List<HelixWorkItemModel> uploadedItems = new List<HelixWorkItemModel>();
        List<HelixWorkItemModel> downloadedItems = new List<HelixWorkItemModel>();

        foreach (var item in allWorkItems)
        {
            Trace.Assert(downloadedItems != null);
            Trace.Assert(uploadedItems != null);
            Trace.Assert(item != null);
        }

        DateTime helixWorkitemDownloadStartTime = DateTime.Now;

        int currentItem = 1;
        int totalItems = allWorkItems.Count;
        int limit = 100;
        DateTime limitStart = DateTime.Now;
        foreach (var item in allWorkItems)
        {
            if (tasks.Count == limit)
            {
                await Task.WhenAll(tasks);
                tasks.Clear();
                
                DateTime limitEnd = DateTime.Now;
                double elapsedLimitSeconds = (limitEnd - limitStart).TotalSeconds;

                Console.WriteLine($"[Helix WorkItem] -- {limit} downloaded in {elapsedLimitSeconds}s");
            }
            Console.WriteLine($"[{currentItem++}:{totalItems}]: Started.");

            Trace.Assert(downloadedItems != null);
            Trace.Assert(uploadedItems != null);
            Trace.Assert(item != null);

            tasks.Add(UploadHelixWorkItemTry(downloadedItems, uploadedItems, item.Item1, item.Item2, item.Item3));
        }

        Console.WriteLine("Starting download helix work items.");
        await Task.WhenAll(tasks);

        DateTime helixWorkitemDownloadEndTime = DateTime.Now;
        double elapsedHelixWorkItemDownloadtime = (helixWorkitemDownloadEndTime - helixWorkitemDownloadStartTime).TotalMinutes;

        Console.WriteLine($"Downloaded {downloadedItems.Count} helix work items in {elapsedHelixWorkItemDownloadtime}m");
        Console.WriteLine($"To upload {uploadedItems.Count}");

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

    private async Task DownloadSubmission(Tuple<string, AzureDevOpsStepModel, AzureDevOpsJobModel> jobTuple, List<HelixSubmissionModel> helixSubmissions, List<Tuple<HelixWorkItemDetail, HelixSubmissionModel, AzureDevOpsJobModel>> allWorkItems, object workItemLock)
    {
        var job = jobTuple.Item1;
        string helixApiString = "https://helix.dot.net/api/2019-06-17/jobs/";

        HelixSubmissionModel model = new HelixSubmissionModel();
        model.Id = Guid.NewGuid().ToString();

        model.Passed = false;
        model.Queues = new List<string>();

        string summaryUri = $"{helixApiString}/{job}";
        string workitemsUri = $"{helixApiString}/{job}/workitems";

        DateTime beginSummary = DateTime.Now;
        string summaryResponse = await Shared.GetAsync(summaryUri);
        DateTime endSummary = DateTime.Now;

        double elapsedSummaryTime = (endSummary - beginSummary).TotalMilliseconds;
        Console.WriteLine($"[Helix] -- [{job}]: Downloaded in {elapsedSummaryTime} ms.");

        HelixWorkItemSummary summary = JsonConvert.DeserializeObject<HelixWorkItemSummary>(summaryResponse);

        if (summary.Finished == null || summary.Created == null)
        {
            return;
        }

        model.End = DateTime.Parse(summary.Finished);
        model.Start = DateTime.Parse(summary.Created);
        model.ElapsedTime = (model.End - model.Start).TotalSeconds;

        model.Name = summary.Name;
        model.Passed = false;
        model.Queues.Add(summary.Properties["operatingSystem"]);
        model.Source = summary.Source;
        model.Type = summary.Type;
        model.StepId = jobTuple.Item2.Id;
        model.JobId = jobTuple.Item3.Id;
        model.JobName = jobTuple.Item3.Name;
        model.RuntimePipelineId = jobTuple.Item3.PipelineId;

        string workItemDetailResponse = null;
        try
        {
            workItemDetailResponse = await Shared.GetAsync(workitemsUri);
        }
        catch (Exception e)
        {
            // Some issue with helix keeps us from downloading this item
            return;
        }

        string workItemJson = workItemDetailResponse;
        List<HelixWorkItemDetail> workItems = JsonConvert.DeserializeObject<List<HelixWorkItemDetail>>(workItemJson);

        Trace.Assert(workItemJson != null);
        model.WorkItemCount = workItems.Count;

        helixSubmissions.Add(model);

        lock(workItemLock)
        {
            foreach (var item in workItems)
            {
                allWorkItems.Add(new Tuple<HelixWorkItemDetail, HelixSubmissionModel, AzureDevOpsJobModel>(item, model, jobTuple.Item3));
            }
        }
    }

    private async Task UploadHelixWorkItemTry(List<HelixWorkItemModel> workItems, List<HelixWorkItemModel> uploadedItems, HelixWorkItemDetail item, HelixSubmissionModel model, AzureDevOpsJobModel jobModel)
    {
        bool failed = false;
        try
        {
            await UploadHelixWorkItem(workItems, uploadedItems, item, model, jobModel);
        }
        catch(Exception e)
        {
            failed = true;
            // First chance.
            Console.WriteLine($"Encountered {e.Message}");
        }

        if (failed)
        {
            // Try again, but do not catch if there is an issue
            await UploadHelixWorkItem(workItems, uploadedItems, item, model, jobModel);
        }
    }

    private async Task UploadHelixWorkItem(List<HelixWorkItemModel> workItems, List<HelixWorkItemModel> uploadedItems, HelixWorkItemDetail item, HelixSubmissionModel model, AzureDevOpsJobModel jobModel)
    {
        DateTime startHelixWorkitem = DateTime.Now;
        string workItemDetailsStr = null;
        try
        {
            workItemDetailsStr = await Shared.GetAsync(item.DetailsUrl);
        }
        catch (Exception e)
        {
            return;
        }

        var modelToAdd = new HelixWorkItemModel();

        HelixWorkItem workItem = JsonConvert.DeserializeObject<HelixWorkItem>(workItemDetailsStr);

        HelixWorkItemModel workItemModel = new HelixWorkItemModel();
        workItemModel.ExitCode = workItem.ExitCode;
        workItemModel.MachineName = workItem.MachineName;
        workItemModel.Name = workItem.Name;
        workItemModel.JobId = jobModel.JobGuid;
        workItemModel.RuntimePipelineId = jobModel.PipelineId;

        workItemModel.Id = Guid.NewGuid().ToString();

        string logUri = null;
        foreach (var log in workItem.Logs)
        {
            if (log["Module"] == "run_client.py")
            {
                logUri = log["Uri"];
                break;
            }
        }

        string testXmlUri = null;
        foreach (var file in workItem.Files)
        {
            if (file["FileName"].Contains(".xml"))
            {
                testXmlUri = file["Uri"];
                break;
            }
        }

        if (testXmlUri != null)
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

                //     foreach (XmlNode test in collection.SelectNodes("test"))
                //     {
                //         string console = null;
                //         if (test.ChildNodes.Count > 0)
                //         {
                //             console = test.ChildNodes[0].InnerText;
                //         }

                //         string testName = test.Attributes["name"].Value;
                //         bool passed = test.Attributes["result"].Value == "Pass" ? true : false;
                //         double timeRun = Double.Parse(test.Attributes["time"].Value);
                        
                //         Test testModel = new Test();

                //         if (passed)
                //         {
                //             testModel.Console = null;
                //         }
                //         else
                //         {
                //             testModel.Console = console;
                //         }

                //         testModel.ElapsedTime = timeRun;
                //         testModel.Name = testName;
                //         testModel.Passed = passed;

                //         testModel.Id = Guid.NewGuid().ToString();
                //         testModel.HelixWorkItemId = workItemModel.Id;

                //         SubmitTestToUpload(testModel);
                //     }
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
                        workItemModel.Console = await Shared.GetAsync(workItem.ConsoleOutputUri);
                    }
                    catch(Exception e)
                    {
                        // do nothing.
                    }
                }

                workItemModel.HelixSubmissionId = model.Id;
                workItemModel.StepId = model.StepId;

                workItemModel.JobName = jobModel.Name;

                SubmitToUpload(workItemModel);
                uploadedItems.Add(workItemModel);

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

        workItems.Add(modelToAdd);
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
