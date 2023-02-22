using Dapper;
using System.Data.SqlClient;
using Z.Dapper.Plus;

namespace Practicals;

public class Final
{
    public static class Connection
    {
        // SQL Server Database Connection String
        public static readonly SqlConnection SqlConnection =
            new("Data Source=DESKTOP-K63766J;Initial Catalog=practical1;Integrated Security=True");
    }

    public class Destination
    {
        public int Id { get; set; }
        public int Sum { get; set; }
        public int Sid { get; set; }
    }

    public static class Global
    {
        public static readonly Dictionary<int, int> MigrationStatus = new();
        public static int q = 1;
    }


    public static void Main(string[] args)
    {
        // Enter range from user
        Console.WriteLine("Enter the range: ");
        var startId = Convert.ToInt32(Console.ReadLine());
        var endId = Convert.ToInt32(Console.ReadLine());

        // Use to give the signal to cancel the migration
        var token = new CancellationTokenSource();

        // Start and run the migration of data in background
        Task backgroundTask = Task.Run(() => { MigrateData(startId, endId, token); });
        
        // Continuously running in the background
        while (true)
        {
            var input = Console.ReadLine();

            if (input == "cancel")
            {
                token.Cancel();
            }
            else if (input == "status")
            {
                GetMigrationStatus();
            }
            else if (input == "exit")
            {
                break;
            }
            else
            {
                Console.WriteLine("Enter Valid Input !");
            }
        }
    }
    
    // Central Handle of all operations
    private static async void MigrateData(int startId, int endId, CancellationTokenSource token)
    {
        // Fetch data from database
        var lst = FetchData(startId, endId);

        // First set all migration status => Ongoing (2)
        foreach (var item in lst)
        {
            Global.MigrationStatus[(int)item.Id] = 2;
        }

        // Create Batch of 100 size -> Insert to Database
        foreach (var item in lst.Chunk(100))
        {
            // Batch of 100 size
            List<Destination> dataBatch = new List<Destination>(100);
            var i = 0;

            foreach (var it in item)
            {
                // If cancel is entered => Cancel current all from 100 Batch to last 
                if (token.IsCancellationRequested)
                {   
                    for (var curr = item.First().Sid; curr <= endId; curr++)
                    {
                        Global.MigrationStatus[curr] = 0;       // <--- Status is 0 for cancelled !
                    }

                    Console.WriteLine("Migration Cancelled...!");
                    return;
                }

                var id = (int)it.Id;
                var firstNumber = (int)it.FirstNumber;
                var secondNumber = (int)it.SecondNumber;
                dataBatch.Insert(i++, new Destination { Id = id + ++Global.q, Sum = firstNumber + secondNumber, Sid = id });

                
                // Delay of 50 milliseconds -> For every migration
                await Task.Delay(50);
            }

            //  Set all migration status => Completed (1)
            foreach (var it in item)
            {
                Global.MigrationStatus[(int)it.Id] = 1;
            }

            // Save 100 Batch data into Destination table
            SaveData(dataBatch);
        }
    }

    // Print Current Status of Migrations
    private static void GetMigrationStatus()
    {
        Console.WriteLine("\nCompleted Migrations: ");
        Console.WriteLine(Global.MigrationStatus.Count(item => item.Value == 1));

        Console.WriteLine("\nOngoing Migrations: ");
        Console.WriteLine(Global.MigrationStatus.Count(item => item.Value == 2));

        Console.WriteLine("\nCancelled Migrations: ");
        Console.WriteLine(Global.MigrationStatus.Count(item => item.Value == 0));

        Console.WriteLine();
    }

    // Fetch data from start and end ID and return in form of List
    private static List<dynamic> FetchData(int startId, int endId)
    {
        var conn = Connection.SqlConnection;
        conn.Open();

        var sql = $"select * from SourceTable where Id between {startId} and {endId};";
        var dataset = conn.Query(sql); // <-- Whole dataset from startID to endID
        conn.Close();
        
        return dataset.ToList();
    }
    
    // Save data into database
    private static void SaveData(List<Destination> dataBatch)
    {
        var conn = Connection.SqlConnection;
        conn.Open();
        DapperPlusManager.Entity<Destination>().Table("DestinationTable");
        conn.BulkInsert(dataBatch);
        conn.Close();
    }
}