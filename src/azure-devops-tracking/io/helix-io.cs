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
using ev27;
using DevOps.Util;

////////////////////////////////////////////////////////////////////////////////

public class HelixIO
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public HelixIO(Container helixContainer, List<string> helixJobs, string jobName, string stepId)
    {
        HelixJobs = helixJobs;

        JobName = jobName;
        StepId = stepId;

        lock(UploadLock)
        {
            if (Uploader == null)
            {
                Action<HelixWorkItemModel> trimDoc = (HelixWorkItemModel document) => {
                    Debug.Assert(document.ToString().Length < 2000000);
                };

                Queue = new TreeQueue<HelixWorkItemModel>(maxLeafSize: 50);
                Uploader = new CosmosUpload<HelixWorkItemModel>("[Helix Work Item Model Upload]", GlobalLock, helixContainer, Queue, (HelixWorkItemModel document) => { return document.Name; }, trimDoc, waitForUpload: true);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    public List<string> HelixJobs { get; set; }
    public string JobName { get; set; }
    public string StepId { get; set; }

    private static object FinishLock = new object();
    private static TreeQueue<HelixWorkItemModel> Queue = null;
    private static CosmosUpload<HelixWorkItemModel> Uploader = null;
    private static object UploadLock = new object();
    private static object GlobalLock = new object();

    ////////////////////////////////////////////////////////////////////////////
    // Member functions
    ////////////////////////////////////////////////////////////////////////////

    public async Task<List<HelixSubmissionModel>> IngestData()
    {
        Debug.Assert(HelixJobs.Count > 0);
        List<HelixSubmissionModel> helixSubmissions = new List<HelixSubmissionModel>();

        foreach (var job in HelixJobs)
        {
            string helixApiString = "https://helix.dot.net/api/2019-06-17/jobs/";

            HelixSubmissionModel model = new HelixSubmissionModel();
            model.Passed = false;
            model.Queues = new List<string>();

            string summaryUri = $"{helixApiString}/{job}";
            string workitemsUri = $"{helixApiString}/{job}/workitems";

            DateTime beginSummary = DateTime.Now;
            string summaryResponse = await Shared.GetAsync(summaryUri, retryCount: 10);
            DateTime endSummary = DateTime.Now;

            double elapsedSummaryTime = (endSummary - beginSummary).TotalMilliseconds;
            Console.WriteLine($"[Helix] -- [{job}]: Downloaded in {elapsedSummaryTime} ms.");

            HelixWorkItemSummary summary = JsonConvert.DeserializeObject<HelixWorkItemSummary>(summaryResponse);

            if (summary.Finished == null || summary.Created == null)
            {
                continue;
            }

            model.End = DateTime.Parse(summary.Finished);
            model.Start = DateTime.Parse(summary.Created);
            model.ElapsedTime = (model.End - model.Start).TotalSeconds;

            model.Name = summary.Name;
            model.Passed = false;
            model.Queues.Add(summary.Properties["operatingSystem"]);
            model.Source = summary.Source;
            model.Type = summary.Type;
            model.WorkItems = new List<HelixWorkItemModel>();
            model.StepId = StepId;

            object modelLock = new object();

            string workItemDetailResponse = null;
            try
            {
                workItemDetailResponse = await Shared.GetAsync(workitemsUri, retryCount: 10);
            }
            catch (Exception e)
            {
                // Some issue with helix keeps us from downloading this item
                continue;
            }

            string workItemJson = workItemDetailResponse;
            List<HelixWorkItemDetail> workItems = JsonConvert.DeserializeObject<List<HelixWorkItemDetail>>(workItemJson);

            Debug.Assert(workItemJson != null);
            model.WorkItemCount = workItems.Count;

            helixSubmissions.Add(model);

            List<Task> tasks = new List<Task>();
            foreach (var item in workItems)
            {
                if (tasks.Count > 100)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }

                tasks.Add(UploadHelixWorkItem(item, modelLock, model));
            }

            await Task.WhenAll(tasks);
        }

        return helixSubmissions;
    }

    public static void SignalToFinish()
    {
        Uploader.Finish();
        Uploader = null;

        Debug.Assert(Uploader == null);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////

    private async Task UploadHelixWorkItem(HelixWorkItemDetail item, object modelLock, HelixSubmissionModel model)
    {
        DateTime startHelixWorkitem = DateTime.Now;
        string workItemDetailsStr = null;
        try
        {
            workItemDetailsStr = await Shared.GetAsync(item.DetailsUrl, retryCount: 10);
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

        string logUri = null;
        foreach (var log in workItem.Logs)
        {
            if (log["Module"] == "run_client.py")
            {
                logUri = log["Uri"];
                break;
            }
        }

        if (logUri != null)
        {
            Debug.Assert(logUri != null);

            string helixRunnerLog = null;
            bool continueDueToException = false;
            try
            {
                helixRunnerLog = await Shared.GetAsync(logUri, retryCount: 10);
            }
            catch (Exception e)
            {
                continueDueToException = true;
            }

            if (continueDueToException)
            {
                return;
            }

            string delim = helixRunnerLog.Contains("_dump_file_upload") ? "\t" : ": ";

            string setupBeginStr = helixRunnerLog.Split(delim)[0];

            if (helixRunnerLog.Contains("dockerhelper"))
            {
                string splitString = helixRunnerLog.Split("write_commands_to_file")[0];
                var splitStringLines = splitString.Split('\n');

                string setupEndStr = splitStringLines[splitStringLines.Length - 1].Split(delim)[0];

                setupBeginStr = Regex.Replace(setupBeginStr, @"\s+", "0");
                setupEndStr = Regex.Replace(setupEndStr, @"\s+", "0");

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
                    setupBeginStr = Regex.Replace(setupBeginStr, @"\s+", "0");
                    setupEndStr = Regex.Replace(setupEndStr, @"\s+", "0");
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
                    Debug.Assert(false);
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
                    setupBeginStr = Regex.Replace(setupBeginStr, @"\s+", "0");
                    setupEndStr = Regex.Replace(setupEndStr, @"\s+", "0");
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
                    Debug.Assert(false);
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
                runtimeEndStr = Regex.Replace(runtimeEndStr, @"\s+", "0");
            }
            

            DateTime runtimeEndTime = DateTime.Parse(runtimeEndStr);

            workItemModel.RunEnd = runtimeEndTime;

            workItemModel.ElapsedSetupTime = (workItemModel.HelixWorkItemSetupEnd - workItemModel.HelixWorkItemSetupBegin).TotalMilliseconds;
            workItemModel.ElapsedRunTime = (workItemModel.RunEnd - workItemModel.RunBegin).TotalMilliseconds;

            Debug.Assert(workItemModel.ElapsedRunTime > 0);
            Debug.Assert(workItemModel.ElapsedSetupTime > 0);

            if (workItemModel.ExitCode != 0)
            {
                workItemModel.Console = null;
                
                try
                {
                    workItemModel.Console = await Shared.GetAsync(workItem.ConsoleOutputUri, retryCount: 10);
                }
                catch(Exception e)
                {
                    // do nothing.
                }
            }
            workItemModel.Id = Guid.NewGuid().ToString();

            workItemModel.JobName = JobName;

            SubmitToUpload(workItemModel);

            modelToAdd.Id = workItemModel.Id;
            modelToAdd.ElapsedRunTime = workItemModel.ElapsedRunTime;
            modelToAdd.ElapsedSetupTime = workItemModel.ElapsedSetupTime;
            modelToAdd.HelixWorkItemSetupBegin = workItemModel.HelixWorkItemSetupBegin;
            modelToAdd.HelixWorkItemSetupEnd = workItemModel.HelixWorkItemSetupEnd;
            modelToAdd.MachineName = workItemModel.MachineName;
            modelToAdd.Name = workItemModel.Name;
            modelToAdd.RunBegin = workItemModel.RunBegin;
            modelToAdd.RunEnd = workItemModel.RunEnd;
            
            lock (modelLock)
            {
                model.WorkItems.Add(modelToAdd);
            }
        }

        DateTime endHelixWorkItem = DateTime.Now;
        double elapsedTime = (endHelixWorkItem - startHelixWorkitem).TotalMilliseconds;

        Console.WriteLine($"[Helix Workitem] -- [{modelToAdd.Name}]:  in {elapsedTime} ms");
    }

    private void SubmitToUpload(HelixWorkItemModel model)
    {
        lock(UploadLock)
        {
            Queue.Enqueue(model);
        }
    }

}