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

		[SetUp]
		public void SetUp() {
			DbConfig config = new DefaultDbConfig();
			system = !DbController.Default.DatabaseExists(config, DatabaseName)
						? DbController.Default.CreateDatabase(config, DatabaseName, AdminUser, AdminPassword)
						: DbController.Default.StartDatabase(config, DatabaseName);

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
	}
}