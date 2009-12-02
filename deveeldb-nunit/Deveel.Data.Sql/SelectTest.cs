using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class SelectTest : TestBase {
		[Test]
		public void SelectIntoSingleVar() {
			using(DeveelDbConnection connection = CreateConnection()) {
				connection.AutoCommit = false;

				DeveelDbCommand command = connection.CreateCommand("name VARCHAR(100) NOT NULL");
				command.ExecuteNonQuery();

				command = connection.CreateCommand("SELECT :name");
				object value = command.ExecuteScalar();

				Console.Out.WriteLine("name = {0}", value);

				command = connection.CreateCommand("SELECT name INTO :name FROM Person");
				command.ExecuteNonQuery();

				Console.Out.WriteLine("SELECT name INTO :name FROM Person");

				command = connection.CreateCommand("SELECT :name");
				value = command.ExecuteScalar();

				Console.Out.WriteLine("name = {0}", value);
			}
		}

		[Test]
		public void SelectIntoTwoVars() {
			using (DeveelDbConnection connection = CreateConnection()) {
				connection.AutoCommit = false;

				DeveelDbCommand command = connection.CreateCommand("name VARCHAR(100) NOT NULL");
				command.ExecuteNonQuery();

				command = connection.CreateCommand("age INTEGER");
				command.ExecuteNonQuery();

				command = connection.CreateCommand("SELECT :name");
				object value = command.ExecuteScalar();

				Console.Out.WriteLine("name = {0}", value);

				command = connection.CreateCommand("SELECT :age");
				value = command.ExecuteScalar();

				Console.Out.WriteLine("age = {0}", value);

				command = connection.CreateCommand("SELECT name, age INTO :name, :age FROM Person");
				command.ExecuteNonQuery();

				Console.Out.WriteLine("SELECT name, age INTO :name, :age FROM Person");

				command = connection.CreateCommand("SELECT :name");
				value = command.ExecuteScalar();

				Console.Out.WriteLine("name = {0}", value);

				command = connection.CreateCommand("SELECT :age");
				value = command.ExecuteScalar();

				Console.Out.WriteLine("age = {0}", value);
			}
		}
	}
}