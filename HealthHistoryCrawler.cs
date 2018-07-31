using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using Microsoft.Extensions.Configuration;

using Newtonsoft.Json;

namespace CeruleanSurprise
{
    public class TableStatusHistory : TableEntity
    {
        public TableStatusHistory(Value historyValue)
        {
            this.PartitionKey = System.Text.RegularExpressions.Regex.Replace(historyValue.Id, "/", "|");
            this.RowKey = historyValue.Properties.OccuredTime.ToString("u");
            this.Resource = historyValue.Id.Split('/')[8];
            this.AvailabilityState = historyValue.Properties.AvailabilityState;
            this.DetailedStatus = historyValue.Properties.DetailedStatus;
            this.ReasonChronicity = historyValue.Properties.ReasonChronicity;
            this.ReasonType = historyValue.Properties.ReasonType;
            this.ResolutionEta = historyValue.Properties.ResolutionEta;
            this.Summary = historyValue.Properties.Summary;
        }

        public TableStatusHistory() { }

        public string Resource { get; set; }
        public string AvailabilityState { get; set; }
        public string ReasonChronicity { get; set; }
        public string DetailedStatus { get; set; }
        public string ReasonType { get; set; }
        public DateTimeOffset? ResolutionEta { get; set; }
        public string Summary { get; set; }
    }

    public partial class StatusHistory
    {
        [JsonProperty("value")]
        public List<Value> Value { get; set; }
    }

    public partial class Value
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("location")]
        public string Location { get; set; }

        [JsonProperty("properties")]
        public Properties Properties { get; set; }
    }

    public partial class Properties
    {
        [JsonProperty("availabilityState")]
        public string AvailabilityState { get; set; }

        [JsonProperty("summary")]
        public string Summary { get; set; }

        [JsonProperty("reasonType", NullValueHandling = NullValueHandling.Ignore)]
        public string ReasonType { get; set; }

        [JsonProperty("reasonChronicity")]
        public string ReasonChronicity { get; set; }

        [JsonProperty("detailedStatus")]
        public string DetailedStatus { get; set; }

        [JsonProperty("occuredTime")]
        public DateTimeOffset OccuredTime { get; set; }

        [JsonProperty("reportedTime", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? ReportedTime { get; set; }

        [JsonProperty("rootCauseAttributionTime", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? RootCauseAttributionTime { get; set; }

        [JsonProperty("resolutionETA", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? ResolutionEta { get; set; }

        [JsonProperty("serviceImpactingEvents", NullValueHandling = NullValueHandling.Ignore)]
        public List<ServiceImpactingEvent> ServiceImpactingEvents { get; set; }
    }

    public partial class ServiceImpactingEvent
    {
        [JsonProperty("eventStartTime")]
        public DateTimeOffset EventStartTime { get; set; }

        [JsonProperty("eventStatusLastModifiedTime")]
        public DateTimeOffset EventStatusLastModifiedTime { get; set; }

        [JsonProperty("correlationId")]
        public Guid CorrelationId { get; set; }

        [JsonProperty("status")]
        public Status Status { get; set; }

        [JsonProperty("incidentProperties")]
        public IncidentProperties IncidentProperties { get; set; }
    }

    public partial class IncidentProperties
    {
        [JsonProperty("title")]
        public string Title { get; set; }

        [JsonProperty("service")]
        public string Service { get; set; }

        [JsonProperty("region")]
        public string Region { get; set; }

        [JsonProperty("incidentType")]
        public string IncidentType { get; set; }
    }

    public partial class Status
    {
        [JsonProperty("value")]
        public string Value { get; set; }
    }

    internal static class Utilities
    {
        internal static HttpClient client = new HttpClient();
        internal static Tuple<string, AzureCredentials, IConfigurationRoot> InitializeApiObjects(Microsoft.Azure.WebJobs.ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            var accessToken = tokenProvider.GetAccessTokenAsync("https://management.azure.com/").Result;

            var credentials = new Microsoft.Rest.TokenCredentials(accessToken, "Bearer");
            var azureCreds = new AzureCredentials(credentials, credentials, credentials.TenantId, AzureEnvironment.AzureGlobalCloud);

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            return new Tuple<string, AzureCredentials, IConfigurationRoot>(accessToken, azureCreds, config);
        }
        internal static async Task<HttpResponseMessage> GetHttpAsyncWithRetry(string url, string bearerToken, TraceWriter log, int retries = 5)
        {
            client.DefaultRequestHeaders.Add("Authorization", $"Bearer {bearerToken}");
            client.DefaultRequestHeaders.Add("Accept", "application/json");

            while (true)
            {
                var result = await client.GetAsync(url);
                if (result.IsSuccessStatusCode)
                {
                    return result;
                }
                else if ((int)result.StatusCode == 429) // We have performed too many requests
                {
                    //Microsoft often returns Retry-After values upwards of twenty minutes, so we ignore it.
                    retries--;
                    await Task.Delay(60000);
                }
                else
                {
                    retries--;
                    await Task.Delay(5000);
                }
                if (retries == 0) { return result; }
            }
        }
    }

    [StorageAccount("AzureWebJobsStorage")]
    public static class HealthHistoryCrawler
    {
        private const string ApiVersion = "2015-01-01";

        private static List<Task<HttpResponseMessage>> GetHealthStatuses(AzureCredentials azureCredentials, string bearerToken, TraceWriter log)
        {
            var azure = Azure.Configure()
                             .Authenticate(azureCredentials)
                             .WithDefaultSubscription();
            var healthTasks = new List<Task<HttpResponseMessage>>();
            foreach (var subscription in azure.Subscriptions.List())
            {
                log.Info($"Fetching subscripiton: {subscription.SubscriptionId}");
                azure = Azure.Configure()
                             .Authenticate(azureCredentials)
                             .WithSubscription(subscription.SubscriptionId);

                var vms = azure.VirtualMachines.List();
                foreach (var vm in vms)
                {
                    string url = $"https://management.azure.com/{vm.Id}/Providers/Microsoft.ResourceHealth/availabilityStatuses?api-version={ApiVersion}";
                    healthTasks.Add(Utilities.GetHttpAsyncWithRetry(url, bearerToken, log));
                }
            }
            return healthTasks;
        }

        private static List<Task<TableResult>> InsertHealthStatuses(List<Task<HttpResponseMessage>> healthTasks, CloudTable table, TraceWriter log)
        {
            var insertTasks = new List<Task<TableResult>>();
            foreach (Task<HttpResponseMessage> task in healthTasks)
            {
                var response = task.Result;
                if (!response.IsSuccessStatusCode) { continue; }
                var payload = response.Content.ReadAsStringAsync().Result;

                StatusHistory history = JsonConvert.DeserializeObject<StatusHistory>(payload);

                foreach (var value in history.Value)
                {
                    //Current statuses have variable timestamps and result in large numbers of duplicate entries
                    if (value.Properties.AvailabilityState == "Available" && value.Id.ToLower().Contains("current")) { continue; }
                    TableStatusHistory tableHistory = new TableStatusHistory(value);
                    TableOperation insert = TableOperation.Insert(tableHistory);
                    insertTasks.Add(table.ExecuteAsync(insert));
                }
            }
            return insertTasks;
        }

        [FunctionName("HealthHistoryCrawler")]
        public static void Run([TimerTrigger("0 30 9 * * *", RunOnStartup = false)]TimerInfo myTimer, TraceWriter log, Microsoft.Azure.WebJobs.ExecutionContext context)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            var clock = new Stopwatch();
            clock.Start();

            var apiObjects = Utilities.InitializeApiObjects(context);

            string bearerToken = apiObjects.Item1;
            AzureCredentials azureCredentials = apiObjects.Item2;
            IConfigurationRoot configuration = apiObjects.Item3;

            var azure = Azure.Configure()
                               .Authenticate(azureCredentials)
                               .WithDefaultSubscription();

            log.Info($"Instantiating table client: {clock.ElapsedMilliseconds}ms");
            var storage = CloudStorageAccount.Parse(configuration["AzureWebJobsStorage"]);
            var tableClient = storage.CreateCloudTableClient();
            var table = tableClient.GetTableReference("ResourceHealthHistory");
            var tableTask = table.CreateIfNotExistsAsync();

            log.Info($"Fetching health statuses: {clock.ElapsedMilliseconds}ms");
            var healthTasks = GetHealthStatuses(azureCredentials, bearerToken, log);
            Task.WaitAll(healthTasks.ToArray());
            log.Info($"Fetched health statuses: {clock.ElapsedMilliseconds}ms");

            tableTask.Wait();
            log.Info($"Inserting statuses: {clock.ElapsedMilliseconds}ms");
            var insertTasks = InsertHealthStatuses(healthTasks, table, log);
            try
            {
                Task.WaitAll(insertTasks.ToArray());
                log.Info($"Inserted: {clock.ElapsedMilliseconds}ms");
            }
            catch (Microsoft.WindowsAzure.Storage.StorageException err) { log.Error($"{err.RequestInformation.ExtendedErrorInformation}"); }
        }
    }
}
