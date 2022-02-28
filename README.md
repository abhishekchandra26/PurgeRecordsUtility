
# A .NET Core console application to delete records from Azure Table Storage which are older than value specified in configuration file.



## About this sample
Delete records from the Azure Table having Timestamp older than value specified in config file.

Set below fields in App.config file:
    <add key="AzureStorageConnectionString" value="" />
    <add key="TableName" value="" />
    <add key="RowKey" value="" />
    <add key="DeleteRecordsOlderThanDays" value="7" />




