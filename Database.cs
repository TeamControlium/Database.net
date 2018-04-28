using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using TeamControlium.Utilities;

namespace TeamControlium.Database
{
    public class DatabaseInterface
    {
        public string CantConnectException { get; private set; }

        protected string _connectionString { get; set; }

        protected string _DatabaseName { get; set; }

        private SqlConnection _connection;
        
        private TimeSpan timeout;

        private TimeSpan interval;

        public static void EnsureDatabaseExists(string databaseLogicalName)
        {
            // Database Logic name points to a Run Options Category where the Category is the name of the database, with the categories options are
            // stored.  Get the actual name of the Database first...
            string databaseName;            
            if (!TestData.Repository.TryGetItem(databaseLogicalName, "DatabaseName", out databaseName))
            {
                throw new Exception($"Database [{databaseLogicalName ?? "No logical name set!!"}] name has not been defined in settings!  Check environment settings for {databaseLogicalName ?? "No logical name set!!"}.DatabaseName");
            }

            string connString;
            if (!TestData.Repository.TryGetItem(databaseLogicalName, "DatabaseConnectionString", out connString))
            {
                throw new Exception($"Database [{databaseLogicalName}] connection string has not been defined in settings!  Check environment settings for {databaseLogicalName}.DatabaseConnectionString");
            }

            try
            {
                // We use the database connection string that has been set, but without the Initial Catalog entry.  We do this incase the db doesnt exist
                // and we need to create it.
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"Connection string: [{connString}].");

                var connStringArray = connString.Split(';');

                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"Removing Initial Catalog (incase database does not exist)");

                var connStringNoCatalog = String.Empty;

                connStringArray.ToList().ForEach(x =>
                {
                    if (x.ToLower().Contains("initial catalog"))
                    {
                        if (x.Split('=')[1] != databaseName)
                            throw new Exception($"Connection String Initial Catalog name ({x.Split('=')[1]}) does not match given database name ({databaseName})");
                    }
                    else
                        connStringNoCatalog += ((string.IsNullOrEmpty(connStringNoCatalog)) ? "" : "; ") + x;
                });

                Logger.WriteLine(Logger.LogLevels.FrameworkInformation, $"Connecting with: [{connStringNoCatalog}].");
                DatabaseInterface db = new DatabaseInterface(databaseName, connStringNoCatalog);

                // If database does not exist create it
                if (!db.DatabaseExists(databaseName))
                {
                    Logger.WriteLine(Logger.LogLevels.FrameworkInformation, $"Database [{databaseName}] does not exist so creating.");

                    // If not testharness or we dont have the folder set, create database but allow SQL Server to decided where to put files.
                    db.Execute($"CREATE DATABASE [{databaseName}]");

                    string LogFileLogicalName = db.GetValue<string>("SELECT name FROM sys.master_files WHERE database_id = db_id(@DBName) and type_desc = 'LOG'", new System.Data.SqlClient.SqlParameter("DBName", databaseName));
                    db.Execute($"ALTER DATABASE [{databaseName}] SET RECOVERY SIMPLE");
                    db.Execute($"ALTER DATABASE [{databaseName}] MODIFY FILE (NAME = '{LogFileLogicalName}', MAXSIZE = 1024MB)");  // 1GB max size of the log...
                }
                else
                {
                    Logger.WriteLine(Logger.LogLevels.FrameworkInformation, $"Database [{databaseName}] does exists so NOT creating.");
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error ensuring {databaseName} database exists: {ex}", ex);
            }
        }

        public DatabaseInterface(string databaseName)
        {
            if (!TestData.Repository.HasCategory(databaseName))
            {
                throw new Exception($"Test Data does not contain any data for database {databaseName ?? "null!"}!");
            }
            else
            {
                _connectionString = TestData.Repository.GetItem<string>(databaseName, "ConnectionString");
                _DatabaseName = databaseName;
            }

            Init();
        }

        public DatabaseInterface(string databaseName, string connectionString)
        {
            _connectionString = connectionString;
            _DatabaseName = databaseName;

            Init();
        }

        public bool DatabaseExists(string name)
        {
            int count = GetValueOrDefault<int>($"SELECT count(*) FROM sys.databases WHERE Name = '{name}'");

            if (count > 1)
                throw new Exception($"More than one database matched name [{name}]!!");

            return (count > 0);
        }

        public bool TableExists(string tableName)
        {
            string query = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tableName}'";
            Logger.Write(Logger.LogLevels.FrameworkDebug, $"Query: [{query}]");

            int dbID = GetValueOrDefault<int>(query);
            Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $" returned [{dbID}]");

            return (dbID > 0);
        }

        public bool CanConnect
        {
            get
            {
                try
                {
                    _connection.Open();
                    CantConnectException = String.Empty;
                    return true;
                }
                catch (Exception ex)
                {
                    CantConnectException = ex.ToString();
                    return false;
                }
                finally
                {
                    if (_connection != null) _connection.Close();
                }
            }
        }

        public T GetValueOrDefault<T>(string query, params SqlParameter[] args)
        {
            object result = GetValue(query, args);
            try
            {
                return (result == null) ? default(T) : (T)result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error casting query [{query}] result", ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public T GetValue<T>(string query, params SqlParameter[] args)
        {
            object result = GetValue(query, args);
            try
            {
                return (T)result;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error casting query [{query}] result", ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public object GetValue(string query, params SqlParameter[] args)
        {
            try
            {
                _connection.Open();
                using (var cmd = new SqlCommand())
                {
                    cmd.CommandText = query;
                    if (args.Length > 0) cmd.Parameters.AddRange(args);
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.Connection = _connection;
                    return cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public List<T> GetValues<T>(string query, params SqlParameter[] args)
        {
            return GetValues(query, args).Cast<T>().ToList();
        }

        public List<object> GetValues(string query, params SqlParameter[] args)
        {
            var result = new List<object>();
            try
            {
                _connection.Open();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = query;
                    if (args.Length > 0) command.Parameters.AddRange(args);
                    using (var reader = command.ExecuteReader())
                    {
                        string[] columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                        if (columns.Length < 1)
                            throw new Exception("No columns returned!");
                        while (reader.Read())
                        {
                            result.Add(reader.IsDBNull(0) ? (object)null : reader[0]);
                        }
                    }
                }
                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public List<T> GetRecords<T>(string query, params SqlParameter[] args)
        {
            var result = new List<T>();
            int row = 0;

            try
            {
                _connection.Open();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = query;
                    if (args.Length > 0) command.Parameters.AddRange(args);
                    using (var reader = command.ExecuteReader())
                    {
                        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                        var objectPublicProperties = typeof(T).GetProperties();
                        while (reader.Read())
                        {
                            var currentRow = new object[reader.FieldCount];
                            reader.GetValues(currentRow);

                            // Create an instance of the record type we want to return then populate all the properties of that class
                            // from the query response data current row
                            var instance = (T)Activator.CreateInstance(typeof(T));
                            for (var cell = 0; cell < currentRow.Length; ++cell)
                            {
                                // If the cell object is marked DBNull, set it to a .NET null
                                if (currentRow[cell] == DBNull.Value)
                                {
                                    currentRow[cell] = null;
                                }

                                // Get the property named the same as the current column of the database query response
                                var namedObjectProperty = objectPublicProperties.SingleOrDefault(x => x.Name.Equals(columns[cell], StringComparison.InvariantCultureIgnoreCase));

                                if (namedObjectProperty != null)
                                {
                                    // If a valid property discover the type of the property.  Nullable types are a pain, so we get the underlying type if nullable
                                    // Then set the value of the property to the value of the query response row/cell
                                    try
                                    {
                                        Type t = Nullable.GetUnderlyingType(namedObjectProperty.PropertyType) ?? namedObjectProperty.PropertyType;
                                        object obj = (currentRow[cell] == null) ? null : Convert.ChangeType(currentRow[cell], t);
                                        namedObjectProperty.SetValue(instance, obj, null);
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception(string.Format("Unable to obtain data from column [{0}] on row {1} of query response data", columns[cell], row), ex);
                                    }
                                }
                            }
                            // Add the row data to the list of typed data 
                            result.Add(instance);
                            row++;
                        }
                    }

                    // Manually clear the SQL command parameters before the end of the using block.  We do this incase any parameter is put on the Large Object Heap and the
                    // .NET garbage collector fails to clean it up due to it being the last generation.  Really just an insurance policy......
                    command.Parameters.Clear();
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public T GetSingleRecord<T>(string query, params SqlParameter[] args)
        {
            TimeSpan timeout;
            if (!TestData.Repository.TryGetItem<TimeSpan>("Database", "Timeout", out timeout))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Option [Database][Timeout] not set; default 30 Seconds being used");
                timeout = TimeSpan.FromSeconds(30);
            }

            TimeSpan interval;
            if (!TestData.Repository.TryGetItem<TimeSpan>("Database", "PollInterval", out interval))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Option [Database][PollInterval] not set; default 1000 milliseconds being used");
                interval = TimeSpan.FromMilliseconds(1000);
            }

            return GetSingleRecord<T>(timeout, interval, query, args);
        }

        public T GetSingleRecord<T>(TimeSpan timeout, TimeSpan interval, string query, params SqlParameter[] args)
        {
            var results = new List<T>();
            try
            {
                var elapsed = Stopwatch.StartNew();
                while (results.Count == 0)
                {
                    results = GetRecords<T>(query, args);
                    if (results.Count > 1)
                        throw new Exception("More than 1 record matched query!");
                    if (results.Count == 1)
                        break;
                    if (elapsed.Elapsed >= timeout)
                    {
                        results.Add(default(T));
                        break;
                    }
                    Thread.Sleep(interval);
                }
                return results[0];
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
        }

        public int ClearTable(string tableName)
        {
            var query = string.Format("DELETE FROM {0}", tableName);
            try
            {
                _connection.Open();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = query;
                    command.CommandType = System.Data.CommandType.Text;
                    return command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        public void DropTable(string tableName)
        {
            try
            {
                var deleteTable = $"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U')) " +
                   "BEGIN " +
                   $"  DROP TABLE {tableName} " +
                   "END";
                Execute(deleteTable);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error dropping table [{tableName}]", ex);
            }
        }

        public int Execute(string query, params SqlParameter[] args)
        {
            try
            {
                _connection.Open();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = query;
                    if (args.Length > 0) command.Parameters.AddRange(args);
                    command.CommandType = System.Data.CommandType.Text;
                    return command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error executing query [{0}]", query), ex);
            }
            finally
            {
                if (_connection != null) _connection.Close();
            }
        }

        private void Init()
        {
            int timeoutMS;
            int intervalMS;

            if (TestData.Repository.TryGetItem("Database", "Timeout", out timeoutMS))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"{timeoutMS} Milliseconds timeout being used for " + _DatabaseName);
                timeout = TimeSpan.FromMilliseconds(timeoutMS);
            }
            else
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Default 30 Seconds timeout being used for " + _DatabaseName);
                timeout = TimeSpan.FromSeconds(30);
            }

            if (TestData.Repository.TryGetItem("Database", "PollInterval", out intervalMS))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"{intervalMS} Milliseconds polling being used for " + _DatabaseName);
                interval = TimeSpan.FromMilliseconds(intervalMS);
            }
            else
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Default 1000 Milliseconds polling being used for " + _DatabaseName);
                interval = TimeSpan.FromMilliseconds(1000);
            }

            if (!string.IsNullOrWhiteSpace(_connectionString))
            {
                _connection = new SqlConnection(_connectionString);
            }
            else
            {
                Logger.Write(Logger.LogLevels.TestInformation, $"{_DatabaseName} - no connection being made as connection string blank or invalid ([{_connectionString}])");
                _connection = null;
            }
        }
    }
}