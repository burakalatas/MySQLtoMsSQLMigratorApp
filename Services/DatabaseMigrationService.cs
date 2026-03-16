using System.Data;
using Microsoft.Data.SqlClient;
using MySqlConnector;

namespace DbMigratorApp.Services
{
    public class DatabaseMigrationService
    {
        public bool IsRunning { get; private set; }
        public bool IsCompleted { get; private set; }
        public string CurrentTable { get; private set; } = "";
        public int ProcessedCount { get; private set; }
        public int TotalCount { get; private set; }
        public string ErrorMessage { get; private set; } = "";

        private Queue<string> _pendingTables = new Queue<string>();
        private string _mySqlConnStr = "";
        private string _msSqlConnStr = "";
        private int _batchSize = 50;

        public async Task MigrateAsync(string mySqlConnStr, string msSqlConnStr, int batchSize = 50, bool isResume = false)
        {
            if (IsRunning) return;

            IsRunning = true;
            IsCompleted = false;
            ErrorMessage = "";

            _mySqlConnStr = mySqlConnStr;
            _msSqlConnStr = msSqlConnStr;
            _batchSize = batchSize;

            try
            {
                using var mySqlConnection = new MySqlConnection(_mySqlConnStr);
                using var msSqlConnection = new SqlConnection(_msSqlConnStr);

                await mySqlConnection.OpenAsync();
                await msSqlConnection.OpenAsync();

                if (!isResume)
                {
                    await ExecuteMySqlNonQueryAsync(mySqlConnection, "SET FOREIGN_KEY_CHECKS=0;");
                    await ExecuteMsSqlNonQueryAsync(msSqlConnection, "EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'");

                    var tables = await GetMySqlTablesAsync(mySqlConnection);
                    _pendingTables = new Queue<string>(tables);
                }

                while (_pendingTables.Count > 0)
                {
                    string table = _pendingTables.Peek();
                    CurrentTable = table;
                    ProcessedCount = 0;
                    TotalCount = await GetTotalRowsAsync(mySqlConnection, table);

                    if (TotalCount > 0)
                    {
                        string pkColumn = await GetPrimaryKeyColumnAsync(mySqlConnection, table);
                        var msSqlColumnTypes = await GetMsSqlColumnTypesAsync(msSqlConnection, table);

                        while (ProcessedCount < TotalCount)
                        {
                            using var selectCmd = new MySqlCommand($"SELECT * FROM `{table}` LIMIT {_batchSize}", mySqlConnection);
                            selectCmd.CommandTimeout = 0;
                            using var reader = await selectCmd.ExecuteReaderAsync();

                            var tempDt = new DataTable();
                            tempDt.Load(reader);

                            if (tempDt.Rows.Count == 0) break;

                            var dt = new DataTable();
                            foreach (DataColumn col in tempDt.Columns)
                            {
                                dt.Columns.Add(col.ColumnName, typeof(object));
                            }

                            foreach (DataRow row in tempDt.Rows)
                            {
                                var newRow = dt.NewRow();
                                foreach (DataColumn col in tempDt.Columns)
                                {
                                    var val = row[col];

                                    if (msSqlColumnTypes.TryGetValue(col.ColumnName, out string dataType) &&
                                        dataType.Equals("uniqueidentifier", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (val != DBNull.Value && val is string strVal && Guid.TryParse(strVal, out Guid parsedGuid))
                                            newRow[col.ColumnName] = parsedGuid;
                                        else
                                            newRow[col.ColumnName] = val;
                                    }
                                    else
                                    {
                                        newRow[col.ColumnName] = val;
                                    }
                                }
                                dt.Rows.Add(newRow);
                            }

                            using (var bulkCopy = new SqlBulkCopy(msSqlConnection, SqlBulkCopyOptions.KeepIdentity, null))
                            {
                                bulkCopy.DestinationTableName = $"[{table}]";
                                bulkCopy.BulkCopyTimeout = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                                }
                                await bulkCopy.WriteToServerAsync(dt);
                            }

                            if (!string.IsNullOrEmpty(pkColumn))
                            {
                                var pkValues = tempDt.AsEnumerable().Select(r => r[pkColumn].ToString());
                                var inClause = string.Join(",", pkValues.Select(v => $"'{v}'"));
                                await ExecuteMySqlNonQueryAsync(mySqlConnection, $"DELETE FROM `{table}` WHERE `{pkColumn}` IN ({inClause})");
                            }

                            ProcessedCount += tempDt.Rows.Count;

                            await Task.Delay(50);
                        }
                    }

                    _pendingTables.Dequeue();
                }

                await ExecuteMySqlNonQueryAsync(mySqlConnection, "SET FOREIGN_KEY_CHECKS=1;");
                await ExecuteMsSqlNonQueryAsync(msSqlConnection, "EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'");

                IsCompleted = true;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                IsRunning = false;
            }
        }

        public void SkipCurrentTableAndContinue()
        {
            if (_pendingTables.Count > 0)
            {
                _pendingTables.Dequeue();
            }

            _ = Task.Run(() => MigrateAsync(_mySqlConnStr, _msSqlConnStr, _batchSize, isResume: true));
        }

        // --- Yardımcı Metotlar ---
        private async Task ExecuteMySqlNonQueryAsync(MySqlConnection conn, string sql)
        {
            using var cmd = new MySqlCommand(sql, conn);
            cmd.CommandTimeout = 0;
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task ExecuteMsSqlNonQueryAsync(SqlConnection conn, string sql)
        {
            using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = 0;
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task<List<string>> GetMySqlTablesAsync(MySqlConnection conn)
        {
            var tables = new List<string>();
            using var cmd = new MySqlCommand("SHOW TABLES", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) tables.Add(reader.GetString(0));
            return tables;
        }

        private async Task<int> GetTotalRowsAsync(MySqlConnection conn, string table)
        {
            using var cmd = new MySqlCommand($"SELECT COUNT(*) FROM `{table}`", conn);
            cmd.CommandTimeout = 0;
            return Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        private async Task<string> GetPrimaryKeyColumnAsync(MySqlConnection conn, string table)
        {
            using var cmd = new MySqlCommand($"SHOW KEYS FROM `{table}` WHERE Key_name = 'PRIMARY'", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync()) return reader.GetString("Column_name");
            return string.Empty;
        }

        private async Task<Dictionary<string, string>> GetMsSqlColumnTypesAsync(SqlConnection conn, string tableName)
        {
            var columnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName";

            using var cmd = new SqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@tableName", tableName);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columnTypes[reader.GetString(0)] = reader.GetString(1);
            }
            return columnTypes;
        }
    }
}
