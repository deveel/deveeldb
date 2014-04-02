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
			DeveelDbCommand command = Connection.CreateCommand("DECLARE CURSOR c_person FOR " +
			                                                   "SELECT name FROM Person WHERE age > 2 " +
			                                                   "   ORDER BY name ASC");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' declared.");

			command = Connection.CreateCommand("OPEN c_person");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Cursor 'c_person' opened");

			command = Connection.CreateCommand("FETCH NEXT FROM c_person");
			DeveelDbDataReader reader = command.ExecuteReader();

			Assert.IsTrue(reader.Read());
			

			Console.Out.WriteLine("c_person.Next.name = {0}", reader.GetString(0));

			reader.Close();

			command = Connection.CreateCommand("name VARCHAR");
			command.ExecuteNonQuery();
			Console.Out.WriteLine("Declared variable :name.");

			command = Connection.CreateCommand("FETCH NEXT FROM c_person INTO :name");
			command.ExecuteNonQuery();

			command = Connection.CreateCommand("SELECT :name");
			object value = command.ExecuteScalar();
			Assert.IsNotNull(value);
			// since we haven't applied any ordering, the first out is the last in
			Assert.IsTrue(value.Equals("David Powell"));

			command = Connection.CreateCommand("CLOSE c_person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("Cursor c_person closed.");
		}
	}
}