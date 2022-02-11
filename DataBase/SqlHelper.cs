using Xunit;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Security;
using System.Threading;

namespace Sql.Integration
{
    public class SqlHelper
    {
        private readonly string _connectionString;
        private readonly SqlCredential _credential;

        public SqlHelper(string catalogName)
        {
            var login = $"login";
            var password = $"password";

            var credential = new SecureString();
            for (var i = 0; i < password.Length; i++)
                credential.InsertAt(i, password[i]);
            credential.MakeReadOnly();
            _credential = new SqlCredential(login, credential);

            _connectionString = $"Data Source=server; " +
                                $"Initial Catalog={catalogName}; ";
        }

        public enum SqlMethod
        {
            // ReSharper disable InconsistentNaming
            SELECT,
            DELETE,
            INSERT,
            UPDATE
        }

        /// <summary>
        /// Gets data from DB through SQL request.
        /// Example: "SELECT * FROM [DB].[namespace].[TableName] WHERE ID = 3704528"
        /// </summary>
        /// <param name="sqlRequest"></param>
        /// <param name="methodType"></param>
        /// <returns></returns>
        public DataTable Execute(SqlMethod methodType, string sqlRequest)
        {
            var dataSet = new DataSet();
            sqlRequest = $"{methodType} {sqlRequest}";
            Console.WriteLine($"[TECH][{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now}] " +
                              $"Sql request => '{sqlRequest}' is ready for send.");

            using (var sqlConnection = new SqlConnection(_connectionString, _credential))
            {
                sqlConnection.Open();

                using var sqlCommand = new SqlCommand(sqlRequest, sqlConnection) { CommandText = sqlRequest };
                new SqlDataAdapter(sqlCommand) { ReturnProviderSpecificTypes = true }.Fill(dataSet);
            }

            if (methodType.Equals(SqlMethod.SELECT) && dataSet.Tables[0].Rows[0] != null)
                return dataSet.Tables[0];

            return null;
        }

        /// <summary>
        /// Checks if some row exists in DB.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="requestData"></param>
        /// <returns></returns>
        public bool DoesRowExist(string tableName, Dictionary<string, string> requestData)
        {
            try
            {
                Execute(SqlMethod.SELECT, $"* FROM {tableName} WHERE {TransformSearchValuesIntoRequest(tableName, requestData)}");
                return true;
            }
            catch (SqlException sqlException)
            {
                throw new Exception(sqlException.Message);
            }
            catch
            {
                Console.WriteLine($"[TECH][{Thread.CurrentThread.ManagedThreadId}][{DateTime.Now}] " +
                                  $"The row with the specified parameters does not exist in the table {tableName}.");
                return false;
            }
        }

        /// <summary>
        /// Sends new insert query to table in DB.
        /// </summary>
        /// Example:
        /// INSERT INTO tableName (requestData.key1, requestData.key2, requestData.key3, ...)
        /// VALUES (requestData.value1, requestData.value2, requestData.value3, ...);
        /// <param name="tableName"></param>
        /// <param name="requestData"></param>
        /// <returns></returns>
        public DataTable Insert(string tableName, Dictionary<string, string> requestData)
        {
            AddQuotesForStringValues(tableName, requestData);

            var columns = string.Empty;
            var values = string.Empty;

            foreach (var data in requestData)
            {
                if (data.Key.StartsWith("convert(varchar(max),["))
                    columns += $"{data.Key},";
                else
                    columns += $"[{data.Key}],";

                if (data.Value.Equals("NULL"))
                    values += "NULL,";
                else
                    values += $"{data.Value},";
            }

            return Execute(SqlMethod.INSERT, $"INTO {tableName} ({columns.TrimEnd(',')}) VALUES ({values.TrimEnd(',')})");
        }

        /// <summary>
        /// Updates row in some table.
        /// Example:
        /// UPDATE table_name
        /// SET column1 = value1, column2 = value2, ...
        /// WHERE condition;
        /// </summary>
        /// <param name="tableName"></param> 
        /// <param name="setData"></param> 
        /// <param name="whereData"></param> 
        /// <returns></returns>
        public DataTable UpdateRow(string tableName, Dictionary<string, string> setData, Dictionary<string, string> whereData)
        {
            var setDataPart = TransformSearchValuesIntoRequest(tableName, setData, true);
            var whereDataPart = TransformSearchValuesIntoRequest(tableName, whereData);

            return Execute(SqlMethod.UPDATE, $"{tableName} SET {setDataPart} WHERE {whereDataPart}");
        }

        /// <summary>
        /// Deletes row from table.
        /// Example: DELETE FROM table_name WHERE condition;
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="requestData"></param>
        public void Delete(string tableName, Dictionary<string, string> requestData) =>
            Execute(SqlMethod.DELETE, $"FROM {tableName} WHERE {TransformSearchValuesIntoRequest(tableName, requestData)}");

        /// <summary>
        /// Selects all rows from table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataTable SelectAll(string tableName) =>
            Execute(SqlMethod.SELECT, $"* FROM {tableName}");

        /// <summary>
        /// Selects row by parameter and executes SQL select command.
        /// Example: SELECT "selectvalue" * FROM "tableName" WHERE "whereValue";
        /// </summary>
        /// <param name="selectValue"></param>
        /// <param name="tableName"></param>
        /// <param name="whereValue"></param>
        /// <returns></returns>
        public DataTable Select(string selectValue, string tableName, string whereValue) =>
            Execute(SqlMethod.SELECT, $"{selectValue} FROM {tableName} WHERE {whereValue}");

        /// <summary>
        ///  Creates dictionary from table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public List<Dictionary<string, string>> GetAllRowsAtTableAsDictionary(string tableName) =>
            ParseDataTableToDictionary(SelectAll(tableName));

        /// <summary>
        /// Parses data from table to dictionary.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public List<Dictionary<string, string>> GetInfoAtDataBase(string request) =>
            ParseDataTableToDictionary(Execute(SqlMethod.SELECT, $" {request}"));

        private static List<Dictionary<string, string>> ParseDataTableToDictionary(DataTable table)
        {
            var result = new List<Dictionary<string, string>>();

            for (var r = 0; r < table.Rows.Count; r++)
            {
                result.Add(new Dictionary<string, string>());
                for (var c = 0; c < table.Columns.Count; c++)
                    result[r].Add(table.Columns[c].ToString(), table.Rows[r].ItemArray[c].ToString());
            }

            return result;
        }

        private string TransformSearchValuesIntoRequest(string tableName, Dictionary<string, string> requestData, bool setPartInUpdateMethod = false)
        {
            var request = string.Empty;

            AddQuotesForStringValues(tableName, requestData);

            if (setPartInUpdateMethod)
                return $"[{requestData.First().Key}]={requestData.First().Value}";

            foreach (var data in requestData)
                if (data.Value.Equals("NULL") && !data.Key.StartsWith("convert(varchar(max),["))
                    request += $" AND [{data.Key}] IS NULL";
                else if (data.Value.Equals("NOT NULL") && !data.Key.StartsWith("convert(varchar(max),["))
                    request += $" AND [{data.Key}] IS NOT NULL";
                else if (data.Key.StartsWith("convert(varchar(max),["))
                    request += $" AND {data.Key}={data.Value}";
                else if (data.Value.StartsWith("'") && data.Value.EndsWith("'"))
                    request += $" AND [{data.Key}] like N{data.Value}";
                else
                    request += $" AND [{data.Key}]={data.Value}";

            return request.Substring(5);
        }

        /// <summary>
        /// Adds single quotes to string values.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="requestData"></param>
        private void AddQuotesForStringValues(string tableName, Dictionary<string, string> requestData)
        {
            var response = Execute(SqlMethod.SELECT, $"TOP (1) * FROM {tableName}");
            var columnNames = requestData.Select(c => c.Key).ToArray();
            var columnTypes = response.Columns.Cast<DataColumn>().ToDictionary(data =>
                data.ColumnName, data => data.DataType.Name);

            for (var i = 0; i < requestData.Count; i++)
                if (requestData[columnNames[i]].Equals("NULL"))
                    continue;
                else if (requestData[columnNames[i]].Equals("NOT NULL"))
                    continue;
                else if (_stringTypes.Contains(columnTypes[columnNames[i]]))
                    requestData[columnNames[i]] = $"'{requestData[columnNames[i]]}'";
                else if (columnTypes[columnNames[i]].Equals(nameof(SqlXml)))
                {
                    var columnAndValue = requestData.ElementAt(i);
                    requestData.Add($"convert(varchar(max),[{columnAndValue.Key}])",
                        $"'{columnAndValue.Value.Replace("'", "''")}'");
                    requestData.Remove(columnAndValue.Key);
                }
                else if (!_nonStringTypes.Contains(columnTypes[columnNames[i]]))
                    throw new Exception(
                        $"Type of column name {columnNames[i]} -> {columnTypes[columnNames[i]]} does not implemented.");
        }

        private static readonly List<string> _nonStringTypes = new List<string>
        {
            nameof(SqlInt16),
            nameof(SqlInt32),
            nameof(SqlInt64),
            nameof(SqlDecimal),
            nameof(SqlBoolean),
            nameof(SqlByte),
            nameof(SqlMoney),
            nameof(SqlSingle),
            nameof(SqlDouble),
            nameof(SqlBinary)
        };

        private static readonly List<string> _stringTypes = new List<string>
        {
            nameof(SqlString),
            nameof(SqlGuid),
            nameof(SqlDateTime),
            nameof(DateTime),
            nameof(SqlBytes)
        };
    }
}