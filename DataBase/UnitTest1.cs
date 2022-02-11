using System;
using Xunit;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Data.SQLite;

namespace DataBase
{
    public class UnitTest1
    {
        [Fact]
        public void CheckSumOfCardsInHammerTime()
        {
            SQLiteConnection db = SQLConnection.Connect(@"DataSource = G:\GitArchive\Database\FirstDataBase.db");
            SQLiteCommand cmd = SQLConnection.Command(db, "Select SUM (quantity) From Decks Where user_id = (Select user_id From Decks Where deck_name = 'Hammer Time');");
            var ob = cmd.ExecuteScalar();
            Assert.Equal(60, Convert.ToInt32(ob));
            SQLConnection.Close(db);
        }

        [Fact]
        public void CheckModernBurnsOwnerName()
        {
            SQLiteConnection db = SQLConnection.Connect(@"DataSource = G:\GitArchive\Database\FirstDataBase.db");
            SQLiteCommand cmd = SQLConnection.Command(db, "Select first_name From Users Where user_id = (Select user_id From Decks Where deck_name = 'Modern Burn');");
            var ob = cmd.ExecuteScalar();
            Assert.Equal("Dmitriy", ob.ToString());
            SQLConnection.Close(db);
        }

        [Fact]
        public void CheckModernBurnOneCopyCard()
        {
            SQLiteConnection db = SQLConnection.Connect(@"DataSource = G:\GitArchive\Database\FirstDataBase.db");
            SQLiteCommand cmd = SQLConnection.Command(db, "Select card_name From Cards Where card_id = (Select card_id From Decks Where deck_name = 'Modern Burn' And quantity = 1)");
            var ob = cmd.ExecuteScalar();
            Assert.Equal("Lurrus of the Dream-Den", ob.ToString());
            SQLConnection.Close(db);
        }

        [Fact]
        public void CheckYaroslavsDeckName()
        {
            SQLiteConnection db = SQLConnection.Connect(@"DataSource = G:\GitArchive\Database\FirstDataBase.db");
            SQLiteCommand cmd = SQLConnection.Command(db, "Select Distinct deck_name From Decks Where user_id = (Select user_id From Users Where first_name = 'Yaroslav')");
            var ob = cmd.ExecuteScalar();
            Assert.Equal("Hammer Time", ob.ToString());
            SQLConnection.Close(db);
        }
    }
}
