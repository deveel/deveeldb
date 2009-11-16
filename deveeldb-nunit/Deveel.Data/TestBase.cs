using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Control;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class TestBase {
		private DbSystem system;

		private const string DatabaseName = "testdb";
		private const string AdminUser = "SA";
		private const string AdminPassword = "pass";

		private static int conn_counter = -1;

		[SetUp]
		public void SetUp() {
			DbController controller = DbController.Default;
			DbConfig config = new DefaultDbConfig();
			system = !controller.DatabaseExists(DatabaseName)
						? controller.CreateDatabase(config, DatabaseName, AdminUser, AdminPassword)
						: controller.StartDatabase(config, DatabaseName);

			OnSetUp();
		}

		[TearDown]
		public void TearDown() {
			OnTearDown();
			system.Close();
		}

		protected virtual void OnSetUp() {
		}

		protected virtual void OnTearDown() {
		}

		protected DeveelDbConnection CreateConnection() {
			return (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
		}

		protected DatabaseConnection CreateDatabaseConnection() {
			string host_string = "Internal/Test/" + conn_counter++;
			User user = system.Database.AuthenticateUser(AdminUser, AdminPassword, host_string);
			return system.Database.CreateNewConnection(user, null);
		}
	}
}