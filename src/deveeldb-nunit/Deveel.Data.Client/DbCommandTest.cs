using System;
using System.Data.Common;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbCommandTest : ContextBasedTest {
		private DeveelDbConnection connection;

		protected override void OnSetUp(string testName) {
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
			DbCommand command = null;

			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);
			Assert.IsNotNull(command.Connection);

			command.CommandText = "a = ?";
			command.Parameters.Add(22);

			Assert.DoesNotThrow(() => command.ExecuteNonQuery());
		}
	}
}
