using System.Data.SqlClient;
using Z.Dapper.Plus;
namespace Practicals;

public static class Connection
{
    // SQL Server Database Connecttion String
    public static readonly SqlConnection SqlConnection =
        new("Data Source=DESKTOP-K63766J;Initial Catalog=practical1;Integrated Security=True");
}
public class Source
{
    public int Id { get; set; }
    public int FirstNumber { get; set; }
    public int SecondNumber { get; set; }
}

class Program
{
    public static int k = 1;

    public static void Main(string[] args)
    {
        BulkInsertData();
    }

    // Inserting Bult data using Dapper Plus
    private static void BulkInsertData()
    {
        var connection = Connection.SqlConnection;
        connection.Open();
        DapperPlusManager.Entity<Source>().Table("SourceTable");

        for (int i = 0; i < 100; i++)
        {
            connection.BulkInsert(GetData());
        }

        connection.Close();
    }

    // Return Data Objects of Type Source Data
    private static List<Source> GetData()
    {
        var data = new List<Source>();
        Random rnd = new Random();
        for (int i = 0; i < 10000; i++)
        {
            data.Add(new Source
            {
                Id = k++,
                FirstNumber = rnd.Next(1, 101),
                SecondNumber = rnd.Next(1, 101)
            });
        }

        return data;
    }
}