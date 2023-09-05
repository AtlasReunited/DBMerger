using System;
using System.Data;
using System.Data.SQLite;

class DatabaseMigrator
{
    private static readonly object dbLock = new object();
    public static void MigrateData(string destinationDbPath, string sourceDbPath, string[] tableNames)
    {
        using (SQLiteConnection destConn = new SQLiteConnection($"Data Source={destinationDbPath};Version=3;"))
        using (SQLiteConnection sourceConn = new SQLiteConnection($"Data Source={sourceDbPath};Version=3;"))
        {
            destConn.Open();
            sourceConn.Open();

            foreach (string tableName in tableNames)
            {
                Console.WriteLine($"Migrating data for table: {tableName}");

                // Query to fetch data from the source database
                string query = $"SELECT * FROM {tableName}";
                using (SQLiteCommand sourceCommand = new SQLiteCommand(query, sourceConn))
                using (SQLiteDataReader reader = sourceCommand.ExecuteReader())
                {
                    // Get the column names from the source table
                    DataTable schemaTable = reader.GetSchemaTable();
                    string[] columnNames = new string[schemaTable.Rows.Count];
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        columnNames[i] = schemaTable.Rows[i]["ColumnName"].ToString();
                    }

                    // Build the INSERT statement for the destination database
                    string insertQuery =
                        $"INSERT OR IGNORE INTO {tableName} ({string.Join(", ", columnNames)}) VALUES ({string.Join(", ", columnNames.Select(c => $"@{c}"))})";
                    using (SQLiteCommand destCommand = new SQLiteCommand(insertQuery, destConn))
                    {
                        while (reader.Read())
                        {
                            // Set parameter values based on column names
                            foreach (string columnName in columnNames)
                            {
                                destCommand.Parameters.AddWithValue($"@{columnName}", reader[columnName]);
                            }

                            // Execute the INSERT statement
                            destCommand.ExecuteNonQuery();
                        }
                    }

                    Console.WriteLine($"Data migration for table {tableName} completed.");
                }
            }
        }
    }
    
    public static async Task MigrateDataAsync(string destinationDbPath, string sourceDbPath, List<string> tableNames)
    {
        using (SQLiteConnection destConn = new SQLiteConnection($"Data Source={destinationDbPath};Version=3;"))
        using (SQLiteConnection sourceConn = new SQLiteConnection($"Data Source={sourceDbPath};Version=3;"))
        {
            await destConn.OpenAsync();
            await sourceConn.OpenAsync();

            foreach (string tableName in tableNames)
            {
                Console.WriteLine($"Migrating data for table: {tableName}");

                await MigrateTableAsync(destConn, sourceConn, tableName);

                Console.WriteLine($"Data migration for table {tableName} completed.");
            }
        }
    }
    private static async Task MigrateTableAsync(SQLiteConnection destConn, SQLiteConnection sourceConn, string tableName)
    {
        string query = $"SELECT * FROM {tableName}";

        using (SQLiteCommand sourceCommand = new SQLiteCommand(query, sourceConn))
        using (SQLiteDataReader reader = (SQLiteDataReader)await sourceCommand.ExecuteReaderAsync())
        {
            // Get the column names from the source table
            DataTable schemaTable = reader.GetSchemaTable();
            string[] columnNames = new string[schemaTable.Rows.Count];
            for (int i = 0; i < schemaTable.Rows.Count; i++)
            {
                columnNames[i] = schemaTable.Rows[i]["ColumnName"].ToString();
            }

            // Build the INSERT statement for the destination database
            string insertQuery = $"INSERT OR IGNORE INTO {tableName} ({string.Join(", ", columnNames)}) VALUES ({string.Join(", ", columnNames.Select(c => $"@{c}"))})";

            while (await reader.ReadAsync())
            {
                using (SQLiteCommand destCommand = new SQLiteCommand(insertQuery, destConn))
                {
                    // Set parameter values based on column names
                    foreach (string columnName in columnNames)
                    {
                        destCommand.Parameters.AddWithValue($"@{columnName}", reader[columnName]);
                    }

                    // Execute the INSERT statement asynchronously
                    await destCommand.ExecuteNonQueryAsync();
                }
            }
        }
    }

}