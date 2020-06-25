////////////////////////////////////////////////////////////////////////////////
//
// Module: shared.cs
//
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;

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

    public static string Get(string uri, int retryCount = 1)
    {
        while (retryCount-- != 0)
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                using(HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using(Stream stream = response.GetResponseStream())
                using(StreamReader reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
            catch (WebException e)
            {
                if (e.Message.Contains("403") || retryCount == 0)
                {
                    throw e;
                }
            }
        }

        return null;
    }

    public static async Task<string> GetAsync(string uri, int retryCount = 5)
    {
        Debug.Assert(Uri.IsWellFormedUriString(uri, UriKind.Absolute));

        if (!Uri.IsWellFormedUriString(uri, UriKind.Absolute))
        {
            throw new Exception("Incorrect uri.");
        }

        int retryIterations = 1;
        while (retryIterations < retryCount + 1)
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
                request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

                using(HttpWebResponse response = (HttpWebResponse)await request.GetResponseAsync())
                using(Stream stream = response.GetResponseStream())
                using(StreamReader reader = new StreamReader(stream))
                {
                    return await reader.ReadToEndAsync();
                }
            }
            catch (WebException e)
            {
                Console.WriteLine($"{e.Message}");
                int timeoutAmount = (int)Math.Pow((double)retryIterations, 2);
                if (timeoutAmount < 10)
                {
                    timeoutAmount = 10;
                }

                if (e.Message.Contains("500"))
                {
                    int i = 0;
                }

                if (e.Message.Contains("403") || retryCount < 0)
                {
                    throw e;
                }
                else
                {
                    Console.WriteLine($"Task Delay {timeoutAmount}");
                    Trace.Assert(timeoutAmount < 4000);
                    await Task.Delay(timeoutAmount);
                    Console.WriteLine("Task resumed.");
                }
            }
        }

        return null;
    }

    public static async Task<string> HttpRequestAsync(string location)
    {
        using (HttpClient client = new HttpClient())
        {
            var response = await client.GetAsync(location);
            return await response.Content.ReadAsStringAsync();
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

            if (itemResponse.Status == TaskStatus.Canceled)
            {
                return new OperationResponse<T>()
                {
                    Item = item,
                    IsSuccessful = false,
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