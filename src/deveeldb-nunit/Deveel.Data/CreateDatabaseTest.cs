using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class CreateDatabaseTest : TestBase {

		[Test]
		public void CreateTables() {
			Console.Out.WriteLine("--- Creating Tables ---");

			DeveelDbConnection connection = CreateConnection();
			GenerateTables(connection);
			connection.Close();
		}

		[Test]
		public void InsertDataPerson() {
			Console.Out.WriteLine("-- Adding to Person Table --");

			DeveelDbConnection connection = CreateConnection();

			if (!TablesGenerated(connection)) {
				Console.Out.WriteLine("Tables were not generated: creating now...");
				GenerateTables(connection);
				Console.Out.WriteLine("Tables generated.");
			}

			Console.Out.WriteLine("Inserting people into the database...");

			DateTime start = DateTime.Now;
			InsertDataPerson(connection);
			connection.Close();
		}

		[Test]
		public void InsertDataMusicGroup() {
			DeveelDbConnection connection = CreateConnection();

			Console.Out.WriteLine("-- Adding to MusicGroup Table --");

			InsertDataMusicGroup(connection);

			connection.Close();
		}

		[Test]
		public void InsertDataListensTo() {
			DeveelDbConnection connection = CreateConnection();

			Console.Out.WriteLine("-- Adding to ListensTo Table --");
			InsertDataListensTo(connection);

			connection.Close();
		}

		[Test]
		public void FullCreate() {
			CreateTables();

			Console.Out.WriteLine("--- Inserting Data ---");

			InsertDataPerson();
			InsertDataMusicGroup();
			InsertDataListensTo();

			Console.Out.WriteLine("--- Complete ---");
		}

		[Test]
		public void OpenDatabase() {
			// we do nothing here cause the SetUp and TearDown methods
			// will open and close the connection...
		}
	}
}