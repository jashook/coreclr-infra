////////////////////////////////////////////////////////////////////////////////
//
// Module: azure-devops-tracking-main.cs
//
// Notes:
//
// Attempt to track everything that is being produced in azure dev ops.
//
////////////////////////////////////////////////////////////////////////////////

using System;
using System.CommandLine.DragonFruit;
using System.Diagnostics;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class AzureDevOpsTrackingMain
{
    public async static Task MainAsync(bool recreateDb = false, 
                                       DateTime? date=null,
                                       bool recalculatePipelineElapsedTime=false,
                                       bool redownloadLogs=false)
    {
        var tracker = new AzureDevopsTracking(recreateDb);

        if (!recalculatePipelineElapsedTime && !redownloadLogs)
        {
            if (date != null)
            {
                await tracker.Remove((DateTime)date);
            }

            await tracker.Update();
        }
        else
        {
            await tracker.Recalculate(recalculatePipelineElapsedTime: recalculatePipelineElapsedTime,
                                      redownloadLogs: redownloadLogs,
                                      begin: date);
        }
    }

    public static int Main(bool recreateDb=false,
                           bool reUpdate=false,
                           string timespan=null,
                           bool recalculatePipelineElapsedTime=false,
                           bool redownloadLogs=false)
    {
        if (reUpdate)
        {
            if (timespan == null)
            {
                Console.WriteLine($"--timespan required with --re-update.");
                return 1;
            }

            DateTime time = DateTime.Now;

            if (timespan.EndsWith('h'))
            {
                Trace.Assert(timespan.StartsWith('-'));

                string amount = timespan.Substring(1, timespan.Length -2);
                int parsedAmount = int.Parse(amount);

                time = time.AddHours(-parsedAmount);
            }
            else if (timespan.EndsWith('d'))
            {
                Trace.Assert(timespan.StartsWith('-'));

                string amount = timespan.Substring(1, timespan.Length -2);
                int parsedAmount = int.Parse(amount);

                time = time.AddDays(-parsedAmount);
            }
            else
            {
                Console.WriteLine("Please pass timespan as -(time)d or -(time)h");
                return 1;
            }

            MainAsync(recreateDb, time).Wait();
        }

        else if (recalculatePipelineElapsedTime ||
                 redownloadLogs)
        {
            DateTime time = DateTime.Now;

            if (timespan.EndsWith('h'))
            {
                Trace.Assert(timespan.StartsWith('-'));

                string amount = timespan.Substring(1, timespan.Length -2);
                int parsedAmount = int.Parse(amount);

                time = time.AddHours(-parsedAmount);
            }
            else if (timespan.EndsWith('d'))
            {
                Trace.Assert(timespan.StartsWith('-'));

                string amount = timespan.Substring(1, timespan.Length -2);
                int parsedAmount = int.Parse(amount);

                time = time.AddDays(-parsedAmount);
            }

            MainAsync(recalculatePipelineElapsedTime: recalculatePipelineElapsedTime,
                      redownloadLogs: redownloadLogs,
                      date: time).Wait();
        }

        else
        {
            MainAsync(recreateDb).Wait();
        }

        return 0;
    }
}