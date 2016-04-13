using System;
using System.Data.Common;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbCommandTest : ContextBasedTest {
		private DeveelDbConnection connection;

		protected override void OnSetUp(string testName, IQuery query) {
			var connString = new DeveelDbConnectionStringBuilder {
				UserName = AdminUserName,
				Password = AdminPassword,
				DataSource = "memory",
				Database = DatabaseName,
				Schema = "APP",
				Create = true
			};

			connection = new DeveelDbConnection(connString);
		}

		[Test]
		public void CreateOnNotOpenConnection() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
		}

		[Test]
		public void CreateEmpty() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand());
			Assert.IsNotNull(command);
			Assert.IsNull(command.Connection);
		}

		[Test]
		public void CreateWithConnectionArgument() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand(connection));
			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);
		}

		[Test]
		public void AttachConnectionToCommand() {
			DbCommand command = null;
			Assert.DoesNotThrow(() => command = new DeveelDbCommand());
			Assert.IsNotNull(command);
			Assert.DoesNotThrow(() => command.Connection = connection);
		}

		[Test]
		public void AssignSimple() {
			DbCommand command = null;

			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := 22";
			Assert.DoesNotThrow(() => command.ExecuteNonQuery());
		}

		[Test]
		public void AssignWithParameters() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := ?";
			command.Parameters.Add(22);

			Assert.DoesNotThrow(() => command.ExecuteNonQuery());
		}

		[Test]
		public void AssignWithSelectScalar() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a := ?";
			command.Parameters.Add(22);

			var result = command.ExecuteScalar();

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<int>(result);
			Assert.AreEqual(22, result);
		}

		[Test]
		public void SelectTables() {
			var command = connection.CreateCommand();

			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "SELECT * FROM INFORMATION_SCHEMA.Tables";

			var reader = command.ExecuteReader();

			Assert.IsNotNull(reader);
			Assert.IsTrue(reader.Read());
			Assert.AreEqual(10, reader.FieldCount);

			var col1 = reader.GetName(0);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_CATALOG", col1);
			var col2 = reader.GetName(1);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_SCHEMA", col2);
			var col3 = reader.GetName(2);
			Assert.AreEqual("INFORMATION_SCHEMA.tables.TABLE_NAME", col3);

			var value = reader.GetValue(2);
		}
	}
}
