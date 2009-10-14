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
			system = !DbController.Default.DatabaseExists(config)
						? DbController.Default.CreateDatabase(config, AdminUser, AdminPassword)
						: DbController.Default.StartDatabase(config);
		}

		[TearDown]
		public void TearDown() {
			system.Close();
		}

		protected DeveelDbConnection CreateConnection() {
			return (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
		}
	}
}