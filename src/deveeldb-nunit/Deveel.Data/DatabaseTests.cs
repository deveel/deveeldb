using System;

using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DatabaseTests {
		private ISystem systemContext;
		private IDatabase database;

		private const string TestDbName = "testdb";
		private const string TestAdminUser = "SA";
		private const string TestAdminPass = "123456";

		[SetUp]
		public void TestSetup() {
			var systemBuilder = new SystemBuilder();
			systemContext = systemBuilder.BuildSystem();

			var test = TestContext.CurrentContext.Test.Name;

			if (test != "CreateNew" &&
				test != "DatabaseNotExists") {
				var dbConfig = new Configuration.Configuration();
				dbConfig.SetValue("database.name", TestDbName);
				database = systemContext.CreateDatabase(dbConfig, TestAdminUser, TestAdminPass);
			}
		}

		[TearDown]
		public void TearDown() {
			database = null;
		}

		[Test]
		public void DatabaseNotExists() {
			Assert.IsFalse(systemContext.DatabaseExists(TestDbName));
		}

		[Test]
		public void CreateNew() {
			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", TestDbName);
			database = systemContext.CreateDatabase(dbConfig, TestAdminUser, TestAdminPass);
		}

		[Test]
		public void AuthenticateAdmin() {
			Assert.DoesNotThrow(() => database.Authenticate(TestAdminUser, TestAdminPass));
		}
	}
}
