using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;

namespace DataBase
{
    public class SQLConnection
    {

        public static SQLiteConnection Connect(string path)
        {
            String connectionString = @path;

            SQLiteConnection db = new SQLiteConnection(connectionString);
            db.Open();
            return db;
        }

        public static SQLiteCommand Command(SQLiteConnection db, string query)
        {
            SQLiteCommand cmd = db.CreateCommand();
            cmd.CommandText = query;
            return cmd;
        }

        public static void Close(SQLiteConnection db)
        {
            db.Close();
        }
    }
}
