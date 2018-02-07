using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using TeamControlium.Utilities;

namespace TeamControlium.Database
{
    public class DatabaseInterface
    {
        public static void EnsureDatabaseExists(string DatabaseLogicalName)
        {
            //
            // Database Logic name points to a Run Options Category where the Category is the name of the database, with the categories options are
            // stored.  Get the actual name of the Database first...
            //
            string databaseName;
            string connString;
            if (!Utilities.TestData.Repository.TryGetItem<string>(DatabaseLogicalName, "DatabaseName", out databaseName))
            {
                throw new Exception($"Database [{DatabaseLogicalName ?? "No logical name set!!"}] name has not been defined in settings!  Check environment settings for {DatabaseLogicalName ?? "No logical name set!!"}.DatabaseName");
            }
            if (!Utilities.TestData.Repository.TryGetItem<string>(DatabaseLogicalName, "DatabaseConnectionString", out connString))
            {
                throw new Exception($"Database [{DatabaseLogicalName}] connection string has not been defined in settings!  Check environment settings for {DatabaseLogicalName}.DatabaseConnectionString");
            }

            try
            {
                //
                // We use the database connection string that has been set, but without the Initial Catalog entry.  We do this incase the db doesnt exist
                // and we need to create it.
                //
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"Connection string: [{connString}].");
                string[] connStringArray = connString.Split(';');
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"Removing Initial Catalog (incase database does not exist)");
                string connStringNoCatalog = "";
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

                //
                // If database does not exist create it
                //
                //
                if (!db.DatabaseExists(databaseName))
                {
                    Logger.WriteLine(Logger.LogLevels.FrameworkInformation, $"Database [{databaseName}] does not exist so creating.");
                    //
                    // If not testharness or we dont have the folder set, create database but allow SQL Server to decided where to put files.
                    //
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
                throw new Exception($"Error ensuring {databaseName} database exists: {ex}");
            }
        }

        public string CantConnectException { get; private set; }
        protected string _connectionString { get; set; }
        private SqlConnection _connection;
        protected string _DatabaseName { get; set; }

        public DatabaseInterface(string DatabaseName)
        {
            if (!Utilities.TestData.Repository.HasCategory(DatabaseName))
            {
                throw new Exception($"Test Data does not contain any data for database {DatabaseName ?? "null!"}!");
            }
            else
            {
                _connectionString = Utilities.TestData.Repository.GetItem<string>(DatabaseName, "ConnectionString");
                _DatabaseName = DatabaseName;
            }
            Init();

        }

        public DatabaseInterface(string DatabaseName, string ConnectionString)
        {
            _connectionString = ConnectionString;
            _DatabaseName = DatabaseName;
            Init();
        }
        public bool DatabaseExists(string name)
        {
            int count = GetValueOrDefault<int>($"SELECT count(*) FROM sys.databases WHERE Name = '{name}'");
            if (count > 1)
                throw new Exception($"More than one database matched name [{name}]!!");
            return (count > 0);
        }

        public bool TableExists(string TableName)
        {
            string query = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{TableName}'";
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
                    CantConnectException = "";
                    return true;
                }
                catch (Exception ex)
                {
                    CantConnectException = ex.ToString();
                    return false;
                }
                finally
                {
                    _connection?.Close();
                }
            }
        }

        public T GetValueOrDefault<T>(string Query, params SqlParameter[] args)
        {
            object result = GetValue(Query, args);
            try
            {
                return (result == null) ? default(T) : (T)result;
            }
            catch (Exception ex) { throw new Exception($"Error casting query [{Query}] result", ex); }
            finally { _connection.Close(); }
        }

        public T GetValue<T>(string Query, params SqlParameter[] args)
        {
            object result = GetValue(Query, args);
            try
            {
                return (T)result;
            }
            catch (Exception ex) { throw new Exception($"Error casting query [{Query}] result", ex); }
            finally { _connection.Close(); }
        }

        public object GetValue(string Query, params SqlParameter[] args)
        {
            try
            {
                _connection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandText = Query;
                if (args.Length > 0) cmd.Parameters.AddRange(args);
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.Connection = _connection;

                return cmd.ExecuteScalar();
            }
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", Query), ex); }
            finally { _connection.Close(); }
        }

        public List<T> GetValues<T>(string Query, params SqlParameter[] args)
        {
            return (List<T>)GetValues(Query, args).Cast<T>().ToList();
        }

        public List<object> GetValues(string Query, params SqlParameter[] args)
        {
            var result = new List<object>();
            string query = Query;

            try
            {
                _connection.Open();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = Query;
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
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", query), ex); }
            finally { _connection.Close(); }
        }

        public List<T> GetRecords<T>(string Query, params SqlParameter[] args)
        {
            var result = new List<T>();
            string query = Query;
            int row = 0;
            try
            {
                _connection.Open();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = Query;
                    if (args.Length > 0) command.Parameters.AddRange(args);
                    using (var reader = command.ExecuteReader())
                    {
                        var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
                        var objectPublicProperties = typeof(T).GetProperties();
                        while (reader.Read())
                        {
                            var currentRow = new object[reader.FieldCount];
                            reader.GetValues(currentRow);

                            //
                            // Create an instance of the record type we want to return then populate all the properties of that class
                            // from the query response data current row
                            //
                            var instance = (T)Activator.CreateInstance(typeof(T));
                            for (var cell = 0; cell < currentRow.Length; ++cell)
                            {
                                //
                                // If the cell object is marked DBNull, set it to a .NET null
                                //
                                if (currentRow[cell] == DBNull.Value)
                                {
                                    currentRow[cell] = null;
                                }

                                //
                                // Get the property named the same as the current column of the database query response
                                ///
                                var namedObjectProperty = objectPublicProperties.SingleOrDefault(x => x.Name.Equals(columns[cell], StringComparison.InvariantCultureIgnoreCase));

                                if (namedObjectProperty != null)
                                {
                                    //
                                    // If a valid property discover the type of the property.  Nullable types are a pain, so we get the underlying type if nullable
                                    // Then set the value of the property to the value of the query response row/cell
                                    //
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
                    //
                    // Manually clear the SQL command parameters before the end of the using block.  We do this incase any parameter is put on the Large Object Heap and the
                    // .NET garbage collector fails to clean it up due to it being the last generation.  Really just an insurance policy......
                    //
                    command.Parameters.Clear();
                }
                return result;
            }
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", query), ex); }
            finally { if (_connection != null) _connection.Close(); }
        }

        public T GetSingleRecord<T>(string Query, params SqlParameter[] args)
        {
            TimeSpan timeout;
            TimeSpan interval;
            if (!Utilities.TestData.Repository.TryGetItem<TimeSpan>("Database", "Timeout", out timeout))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Option [Database][Timeout] not set; default 30 Seconds being used");
                timeout = TimeSpan.FromSeconds(30);
            }
            if (!Utilities.TestData.Repository.TryGetItem<TimeSpan>("Database", "PollInterval", out interval))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Option [Database][PollInterval] not set; default 1000 milliseconds being used");
                interval = TimeSpan.FromMilliseconds(1000);
            }
            return GetSingleRecord<T>(timeout, interval, Query, args);
        }

        public T GetSingleRecord<T>(TimeSpan Timeout, TimeSpan Interval, string Query, params SqlParameter[] args)
        {
            var results = new List<T>();
            try
            {
                Stopwatch elapsed = Stopwatch.StartNew();
                while (results.Count == 0)
                {
                    results = GetRecords<T>(Query, args);
                    if (results.Count > 1)
                        throw new Exception("More than 1 record matched query!");
                    if (results.Count == 1)
                        break;
                    if (elapsed.Elapsed >= Timeout)
                    {
                        results.Add(default(T));
                        break;
                    }
                    //                    throw new Exception($"Query returned no results after {elapsed.Elapsed.TotalSeconds.ToString()} seconds!");
                    Thread.Sleep(Interval);
                }
                return results[0];
            }
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", Query), ex); }
        }

        public int ClearTable(string TableName)
        {
            string query = string.Format("DELETE FROM {0}", TableName);
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
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", query), ex); }
            finally { _connection.Close(); }
        }

        public void DropTable(string TableName)
        {
            try
            {
                string deleteTable = $"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{TableName}]') AND type in (N'U')) " +
                   "BEGIN " +
                   $"  DROP TABLE {TableName} " +
                   "END";
                Execute(deleteTable);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error dropping table [{TableName}]", ex);
            }
        }

        public int Execute(string Query, params SqlParameter[] args)
        {
            string query = Query;
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
            catch (Exception ex) { throw new Exception(string.Format("Error executing query [{0}]", query), ex); }
            finally { if (_connection != null) _connection.Close(); }
        }

        private void Init()
        {
            int timeoutMS;
            int intervalMS;

            if (Utilities.TestData.Repository.TryGetItem<int>("Database", "Timeout", out timeoutMS))
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, $"{timeoutMS} Milliseconds timeout being used for " + _DatabaseName);
                timeout = TimeSpan.FromMilliseconds(timeoutMS);
            }
            else
            {
                Logger.WriteLine(Logger.LogLevels.FrameworkDebug, "Default 30 Seconds timeout being used for " + _DatabaseName);
                timeout = TimeSpan.FromSeconds(30);
            }

            if (Utilities.TestData.Repository.TryGetItem<int>("Database", "PollInterval", out intervalMS))
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

        private TimeSpan timeout;
        private TimeSpan interval;

    }
}
