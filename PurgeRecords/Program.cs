using PurgeRecords.Services;
using System;

namespace PurgeRecords
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            PurgeService purge = new PurgeService();
            await purge.PurgeFromAzure();
        }
    }
}
