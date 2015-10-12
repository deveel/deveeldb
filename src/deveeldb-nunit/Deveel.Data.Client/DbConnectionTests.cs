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
	}
}
