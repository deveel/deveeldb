using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CursorTest : TestBase {
		protected override bool RequiresSchema {
			get { return true; }
		}

		[Test]
		public void TestCursor() {
			DeveelDbCommand command = Connection.CreateCommand("DECLARE c_person CURSOR FOR " +
			                                                   "SELECT * FROM Person WHERE age > 2 " +
			                                                   "   ORDER BY name ASC");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' declared.");

			command = Connection.CreateCommand("OPEN c_person");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' opened");

			command = Connection.CreateCommand("FETCH NEXT FROM c_person");
			DeveelDbDataReader reader = command.ExecuteReader();

			Assert.IsTrue(reader.Read());
			

			Console.Out.WriteLine("c_person.Next.name = {0}", reader.GetString(1));

			reader.Close();

			command = Connection.CreateCommand("name VARCHAR");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Declared variable :name.");

			command = Connection.CreateCommand("FETCH NEXT FROM c_person INTO :name");
			//TODO: assert the value
			command.ExecuteScalar();

			command = Connection.CreateCommand("CLOSE c_person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("Cursor c_person closed.");
		}

		[Test]
		public void Declare() {
			const string commandText = "DECLARE person_cursor CURSOR FOR " +
									   "SELECT * FROM Person WHERE age > 2 " +
									   "   ORDER BY name ASC";

			DeveelDbCommand command = Connection.CreateCommand(commandText);
			Assert.AreEqual(0, command.ExecuteNonQuery());
		}

		[Test]
		public void Open() {
			DeveelDbCommand command = Connection.CreateCommand("OPEN c_person");
			Assert.AreEqual(0, command.ExecuteNonQuery());
		}

		[Test]
		public void FetchNext() {
			DeveelDbCommand command = Connection.CreateCommand("FETCH NEXT FROM c_person");

			using (DeveelDbDataReader reader = command.ExecuteReader()) {
				while (reader.Read()) {
					Console.Out.WriteLine("Name: {0}", reader.GetString(1));
				}
			}
		}

		[Test]
		public void Close() {
			DeveelDbCommand command = Connection.CreateCommand("CLOSE c_person");
			Assert.AreEqual(1, command.ExecuteNonQuery());
		}
	}
}