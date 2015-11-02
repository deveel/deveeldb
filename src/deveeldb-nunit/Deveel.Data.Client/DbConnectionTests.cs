using System;
using System.Data;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbConnectionTests : ContextBasedTest {
		[Test]
		public void OpenForAdmin() {
			IDbConnection connection = null;
			Assert.DoesNotThrow(() => connection = Database.CreateDbConnection(AdminUserName, AdminPassword));
			Assert.IsNotNull(connection);

			Assert.AreEqual(ConnectionState.Closed, connection.State);
			Assert.DoesNotThrow(() => connection.Open());
			Assert.AreEqual(ConnectionState.Open, connection.State);
			Assert.DoesNotThrow(() => connection.Close());
			Assert.AreEqual(ConnectionState.Closed, connection.State);
		}

		[Test]
		public void QueryScalarForAdmin() {
			IDbConnection connection = null;
			Assert.DoesNotThrow(() => connection = Database.CreateDbConnection(AdminUserName, AdminPassword));
			Assert.IsNotNull(connection);

			IDbCommand command = null;
			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);

			command.CommandText = "SELECT user()";

			object value = null;
			Assert.DoesNotThrow(() => value = command.ExecuteScalar());
			Assert.IsNotNull(value);
			Assert.IsInstanceOf<string>(value);
			Assert.AreEqual(AdminUserName, value);

			Assert.DoesNotThrow(() => connection.Dispose());
		}

		[Test]
		public void QueryForAdmin() {
			IDbConnection connection = null;
			Assert.DoesNotThrow(() => connection = Database.CreateDbConnection(AdminUserName, AdminPassword));
			Assert.IsNotNull(connection);

			IDbCommand command = null;
			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);

			command.CommandText = "SELECT user()";

			IDataReader reader = null;
			Assert.DoesNotThrow(() => reader = command.ExecuteReader());
			Assert.IsNotNull(reader);
			Assert.AreEqual(1, reader.FieldCount);
			Assert.IsTrue(reader.Read());

			object value = null;
			Assert.DoesNotThrow(() => value = reader.GetValue(0));
			Assert.IsInstanceOf<string>(value);
		}
	}
}
