////////////////////////////////////////////////////////////////////////////////
//
// Module: shared.cs
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

public class Shared
{
    public static async Task<BulkOperationResponse<T>> ExecuteTasksAsync<T>(IReadOnlyList<Task<OperationResponse<T>>> tasks)
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

    public static async Task<string> HttpRequest(string location)
    {
        using (HttpClient client = new HttpClient())
        {
            var response = await client.GetAsync(location);
            return await response.Content.ReadAsStringAsync();
        }
    }

    private static long HttpRequestCount = 0;

    public static async Task HttpRequest(string location, Func<string, Task<bool>> htmlResponse) 
    {
        CancellationToken cancelToken = default(CancellationToken);
        using (HttpClient client = new HttpClient())
        {
            bool retry = false;
            int retryCount = 5;
            do
            {
                DateTime begin = DateTime.Now;
                var response = await client.GetAsync(location, cancelToken);
                var responseHtml = await response.Content.ReadAsStringAsync();
                DateTime end = DateTime.Now;

                double elapsedTime = (end - begin).TotalMilliseconds;
                Console.WriteLine($"[HttpRequest: {++HttpRequestCount}]: Finished in {elapsedTime} milliseconds");

                await Task.Run(async () => { retry = await htmlResponse(responseHtml); });
            } while (retry && --retryCount > 0);

            if (retry && retryCount < 0)
            {
                throw new Exception("Unable to download helix results.");
            }
        }
    }
}

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