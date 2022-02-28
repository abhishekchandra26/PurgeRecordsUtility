using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

using Microsoft.Azure.Cosmos.Table;
using System.Linq;
using PurgeRecords.BusinessObjects;
using System.Threading.Tasks.Dataflow;

namespace PurgeRecords.Services
{
    public class PurgeService
    {
        private const string AppName = "SG.TLog.TransPurge.Sync";
        private string azureStorageConnectionString;
        private static CloudStorageAccount cloudStorageAccount;
        private static CloudTableClient tableClient;
        private readonly string tableName;
        private readonly string rowKey;
        private readonly int deleteRecordsOlderThanDays;
        public PurgeService()
        {
            azureStorageConnectionString = ConfigurationManager.AppSettings["AzureStorageConnectionString"];
            cloudStorageAccount = CloudStorageAccount.Parse(azureStorageConnectionString);
            tableClient = cloudStorageAccount.CreateCloudTableClient();
            tableName = ConfigurationManager.AppSettings["TableName"];
            rowKey = ConfigurationManager.AppSettings["RowKey"];
            deleteRecordsOlderThanDays = int.Parse(ConfigurationManager.AppSettings["DeleteRecordsOlderThanDays"]);
        }

        public async Task PurgeFromAzure()
        {
            CloudTable table = tableClient.GetTableReference(tableName);
            var filterDate = DateTime.UtcNow.Date.AddDays(-(deleteRecordsOlderThanDays));
            List<MyAzureTable> myAzureTableEntities = await GetAzureTableEntitiesAsync(tableName, cloudStorageAccount, filterDate);
            List<string> partitionKeys = myAzureTableEntities.Select(x => x.PartitionKey).Distinct().ToList();
            var block = new ActionBlock<(string partitionKey, CloudTable cloudTable)>(
                  async x => await DeleteAzureTableEntitiesAsync(x.partitionKey, x.cloudTable, filterDate),
                  new ExecutionDataflowBlockOptions
                  {
                      BoundedCapacity = 100, // Cap the item count
                      MaxDegreeOfParallelism = 16
                  });

            foreach (string partitionKey in partitionKeys)
            {
                await block.SendAsync((partitionKey, table));
            }

            block.Complete();
            await block.Completion;
            Console.WriteLine("Total Records : {0} ", Convert.ToString(myAzureTableEntities.Count()));
        }

        public async Task<List<MyAzureTable>> GetAzureTableEntitiesAsync(string tableName, CloudStorageAccount storageAccount, DateTime timestamp)
        {
            List<MyAzureTable> myAzureTableEntities = new List<MyAzureTable>();

            CloudTable cloudTable = tableClient.GetTableReference(tableName);

            TableQuery<MyAzureTable> getQuery = new TableQuery<MyAzureTable>()
               .Where(TableQuery.CombineFilters(
                   TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey),
                   TableOperators.And,
                   TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThanOrEqual, timestamp)
                   )
               );


            TableContinuationToken continuationToken = null;

            do
            {
                var tableQueryResult = await cloudTable.ExecuteQuerySegmentedAsync(getQuery, continuationToken);

                continuationToken = tableQueryResult.ContinuationToken;

                foreach (MyAzureTable item in tableQueryResult)
                {
                    myAzureTableEntities.Add(item);
                }
            }
            while (continuationToken != null);

            return myAzureTableEntities;
        }

        public async Task DeleteAzureTableEntitiesAsync(string partitionKey, CloudTable table, DateTime filterDate)
        {
            TableQuery<TableEntity> deleteQuery = new TableQuery<TableEntity>()
                .Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan, filterDate)
                    )
                )
                .Select(new string[] { "PartitionKey", "RowKey" });

            TableContinuationToken continuationToken = null;

            do
            {
                var tableQueryResult = table.ExecuteQuerySegmentedAsync(deleteQuery, continuationToken);

                continuationToken = tableQueryResult.Result.ContinuationToken;

                // Split into chunks of 100 for batching
                List<List<TableEntity>> rowsChunked = tableQueryResult.Result.Select((x, index) => new { Index = index, Value = x })
                    .Where(x => x.Value != null)
                    .GroupBy(x => x.Index / 100)
                    .Select(x => x.Select(v => v.Value).ToList())
                    .ToList();

                // Delete each chunk of 100 in a batch
                foreach (List<TableEntity> rows in rowsChunked)
                {
                    TableBatchOperation tableBatchOperation = new TableBatchOperation();
                    rows.ForEach(x => tableBatchOperation.Add(TableOperation.Delete(x)));

                    await table.ExecuteBatchAsync(tableBatchOperation);
                    Console.WriteLine("PartitionKey: {0} ", partitionKey);
                }
            }
            while (continuationToken != null);
        }
    }
}
