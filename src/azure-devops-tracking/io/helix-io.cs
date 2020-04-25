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
using DevOps.Util;

////////////////////////////////////////////////////////////////////////////////

public class HelixIO
{
    ////////////////////////////////////////////////////////////////////////////
    // Constructor
    ////////////////////////////////////////////////////////////////////////////

    public HelixIO(Container helixContainer, List<string> helixJobs, string jobName)
    {
        HelixContainer = helixContainer;
        HelixJobs = helixJobs;
        CosmosOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();
        CosmosRetryOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();

        FailedDocumentCount = 0;
        SuccessfulDocumentCount = 0;

        JobName = jobName;

        DocumentSize = 0;
        RetrySize = 0;

        ActiveTasks = 0;

        // There is a ~2mb limit for size and there can be roughly 200 active
        // tasks at one time.
        CapSize = (long)((1 * 1000 * 1000) * 1.5);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////////////////////

    public int ActiveTasks { get; set; }
    public long CapSize { get; set; }
    public List<Task<OperationResponse<HelixWorkItemModel>>> CosmosOperations { get; set; }
    public List<Task<OperationResponse<HelixWorkItemModel>>> CosmosRetryOperations { get; set; }
    public long DocumentSize { get; set; }
    public int FailedDocumentCount { get; set; }
    public Container HelixContainer { get; set; }
    public List<string> HelixJobs { get; set; }
    public string JobName { get; set; }
    public long RetrySize { get; set; }
    public int SuccessfulDocumentCount { get; set; }

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
            
            string summaryResponse = await Shared.HttpRequest(summaryUri);

            HelixWorkItemSummary summary = JsonConvert.DeserializeObject<HelixWorkItemSummary>(summaryResponse);

            model.End = DateTime.Parse(summary.Finished);
            model.Start = DateTime.Parse(summary.Created);
            model.ElapsedTime = (model.End - model.Start).TotalSeconds;

            model.Name = summary.Name;
            model.Passed = false;
            model.Queues.Add(summary.Properties["operatingSystem"]);
            model.Source = summary.Source;
            model.Type = summary.Type;
            model.WorkItems = new List<HelixWorkItemModel>();

            string workItemDetailResponse = await Shared.HttpRequest(workitemsUri);

            string workItemJson = workItemDetailResponse;
            List<HelixWorkItemDetail> workItems = JsonConvert.DeserializeObject<List<HelixWorkItemDetail>>(workItemJson);

            Debug.Assert(workItemJson != null);
            model.WorkItemCount = workItems.Count;

            helixSubmissions.Add(model);

            List<Task> tasks = new List<Task>();
            foreach (var item in workItems)
            {
                if (tasks.Count > 30)
                {
                    await Task.WhenAll(tasks);
                    tasks = new List<Task>();
                }

                tasks.Add(Shared.HttpRequest(item.DetailsUrl, async (workItemDetailsStr) => {
                    if (workItemDetailsStr.Contains("An error has occurred."))
                    {
                        // Failed. Should be retried
                        return true;
                    }

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

                        string helixRunnerLog = await Shared.HttpRequest(logUri);

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

                        workItemModel.Console = await Shared.HttpRequest(workItem.ConsoleOutputUri);
                        workItemModel.Id = Guid.NewGuid().ToString();

                        workItemModel.JobName = JobName;

                        await AddOption(workItemModel);

                        var modelToAdd = new HelixWorkItemModel();
                        modelToAdd.Id = workItemModel.Id;
                        modelToAdd.ElapsedRunTime = workItemModel.ElapsedRunTime;
                        modelToAdd.ElapsedSetupTime = workItemModel.ElapsedSetupTime;
                        modelToAdd.HelixWorkItemSetupBegin = workItemModel.HelixWorkItemSetupBegin;
                        modelToAdd.HelixWorkItemSetupEnd = workItemModel.HelixWorkItemSetupEnd;
                        modelToAdd.MachineName = workItemModel.MachineName;
                        modelToAdd.Name = workItemModel.Name;
                        modelToAdd.RunBegin = workItemModel.RunBegin;
                        modelToAdd.RunEnd = workItemModel.RunEnd;
                        
                        model.WorkItems.Add(modelToAdd);
                    }

                    return false;
                }));
            }

            await Task.WhenAll(tasks);
        }

        await DrainCosmosOperations();
        await DrainRetryOperations();

        return helixSubmissions;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helper functions
    ////////////////////////////////////////////////////////////////////////////

    private async Task AddOption(HelixWorkItemModel document)
    {
        if (document.Console != null)
        {
            if (document.Console.Length > 1000*1000)
            {
                document.Console = document.Console.Substring(0, 1000*1000);
            }

            DocumentSize += document.Console.Length;
        }
        
        CosmosOperations.Add(HelixContainer.CreateItemAsync<HelixWorkItemModel>(document, new PartitionKey(document.Name)).CaptureOperationResponse(document));
        
        if (DocumentSize > CapSize || CosmosOperations.Count > 50)
        {
            await DrainCosmosOperations();
            DocumentSize = 0;
            CosmosOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();
        }

        ++ ActiveTasks;
    }

    private async Task AddRetryOption(HelixWorkItemModel document)
    {
        if (document.Console != null)
        {
            RetrySize += document.Console.Length;
        }
        CosmosRetryOperations.Add(HelixContainer.CreateItemAsync<HelixWorkItemModel>(document, new PartitionKey(document.Name)).CaptureOperationResponse(document));

        if (RetrySize > CapSize || CosmosRetryOperations.Count > 50)
        {
            await DrainRetryOperations();
            RetrySize = 0;
            CosmosRetryOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();
        }

        ++ ActiveTasks;
    }

    private async Task DrainCosmosOperations()
    {
        ActiveTasks -= CosmosOperations.Count;

        List<Task<OperationResponse<HelixWorkItemModel>>> copy = new List<Task<OperationResponse<HelixWorkItemModel>>>();

        try
        {
            foreach (var item in CosmosOperations)
            {
                copy.Add(item);
            }
        }
        catch (Exception e)
        {
            foreach (var item in CosmosOperations)
            {
                copy.Add(item);
            }
        }

        CosmosOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();
        BulkOperationResponse<HelixWorkItemModel> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(copy);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await AddRetryOption(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

    private async Task DrainRetryOperations()
    {
        ActiveTasks -= CosmosRetryOperations.Count;

        List<Task<OperationResponse<HelixWorkItemModel>>> copy = new List<Task<OperationResponse<HelixWorkItemModel>>>();

        foreach (var item in CosmosRetryOperations)
        {
            copy.Add(item);
        }

        CosmosRetryOperations = new List<Task<OperationResponse<HelixWorkItemModel>>>();

        BulkOperationResponse<HelixWorkItemModel> helixBulkOperationResponse = await Shared.ExecuteTasksAsync(copy);
        Console.WriteLine($"Bulk update operation finished in {helixBulkOperationResponse.TotalTimeTaken}");
        Console.WriteLine($"Consumed {helixBulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
        Console.WriteLine($"Created {helixBulkOperationResponse.SuccessfulDocuments} documents");
        Console.WriteLine($"Failed {helixBulkOperationResponse.Failures.Count} documents");

        if (helixBulkOperationResponse.Failures.Count > 0)
        {
            Console.WriteLine($"First failed sample document {helixBulkOperationResponse.Failures[0].Item1.Name} - {helixBulkOperationResponse.Failures[0].Item2}");

            foreach (var operationFailure in helixBulkOperationResponse.Failures)
            {
                await AddRetryOption(operationFailure.Item1);
            }
        }

        SuccessfulDocumentCount += helixBulkOperationResponse.SuccessfulDocuments;
        FailedDocumentCount += helixBulkOperationResponse.Failures.Count;
    }

}