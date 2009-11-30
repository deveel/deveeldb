using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CursorTest : TestBase {
		private DeveelDbConnection connection;

		protected override void OnSetUp() {
			connection = CreateConnection();
			connection.AutoCommit = false;
		}

		protected override void OnTearDown() {
			connection.Dispose();
		}

		[Test]
		public void TestCursor() {
			DeveelDbCommand command = connection.CreateCommand("DECLARE c_person CURSOR FOR " +
			                                                   "SELECT * FROM Person WHERE age > 2 " +
			                                                   "   ORDER BY name ASC");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' declared.");

			command = connection.CreateCommand("OPEN c_person");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' opened");

			command = connection.CreateCommand("FETCH NEXT FROM c_person");
			DeveelDbDataReader reader = command.ExecuteReader();

			if (reader.Read())
				Console.Out.WriteLine("c_person.Next.name = {0}", reader.GetString(1));

			reader.Close();

			command = connection.CreateCommand("CLOSE c_person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("Cursor c_person closed.");
		}

		[Test]
		public void Declare() {
			const string commandText = "DECLARE person_cursor CURSOR FOR " +
									   "SELECT * FROM Person WHERE age > 2 " +
									   "   ORDER BY name ASC";

			DeveelDbCommand command = connection.CreateCommand(commandText);
			Assert.AreEqual(1, command.ExecuteNonQuery());
		}

		[Test]
		public void Open() {
			DeveelDbCommand command = connection.CreateCommand("OPEN c_person");
			Assert.AreEqual(1, command.ExecuteNonQuery());
		}

		[Test]
		public void FetchNext() {
			DeveelDbCommand command = connection.CreateCommand("FETCH NEXT FROM c_person");

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					Console.Out.WriteLine("Name: {0}", reader.GetString(1));
				}
			}
		}

		[Test]
		public void Close() {
			DeveelDbCommand command = connection.CreateCommand("CLOSE c_person");
			Assert.AreEqual(1, command.ExecuteNonQuery());
		}
	}
}