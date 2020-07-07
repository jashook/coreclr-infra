using System;
using System.Collections.Generic;
using System.Diagnostics;

using Kusto.Data;
using Kusto.Data.Common;

namespace kusto
{
    class JobSubmission
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

    class WorkItemDetails
    {
        public string WorkItemFriendlyName;
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

    class Program 
    {
        static void Main(string[] args)
        {
            string token = Environment.GetEnvironmentVariable("kustoKey");
            string clientId = Environment.GetEnvironmentVariable("clientId");
            string authority = Environment.GetEnvironmentVariable("authorityId");
            string connectionString = $"Data Source=https://engsrvprod.kusto.windows.net:443;AAD Federated Security=True;Application Client Id={clientId};Application Key={token};Authority ID={authority}";
            
            var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider(connectionString);
            var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };

            string buildId = "20200617.120";

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
            using (var reader = client.ExecuteQuery("engineeringdata", query, clientRequestProperties))
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

                    workItems.Add(details.WorkItemName, details);
                }
            }

            int i = 0;
        }
    }
}
