using System;

using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DatabaseTests {
		private ISystemContext systemContext;
		private IDatabaseContext databaseContext;
		private IDatabase database;

		private const string TestDbName = "testdb";
		private const string TestAdminUser = "SA";
		private const string TestAdminPass = "123456";

		[SetUp]
		public void TestSetup() {
			systemContext = new SystemContext();

			var test = TestContext.CurrentContext.Test.Name;
			if (test != "CreateNewContext") {
				databaseContext = new DatabaseContext(systemContext, TestDbName);
			}

			if (test != "CreateNew" &&
				test != "DatabaseNotExists" &&
				test != "CreateNewContext") {
				database = new Database(databaseContext);
				database.Create(TestAdminUser, TestAdminPass);
				database.Open();
			}
		}

		[TearDown]
		public void TearDown() {
			database = null;
		}

		[Test]
		public void CreateNewContext() {
			IDatabaseContext context = null;
			Assert.DoesNotThrow(() => context = new DatabaseContext(systemContext,TestDbName));
			Assert.IsNotNull(context);
			Assert.AreEqual(TestDbName, context.DatabaseName());
			Assert.IsInstanceOf<InMemoryStorageSystem>(context.StoreSystem);
		}

		[Test]
		public void DatabaseNotExists() {
			database = new Database(databaseContext);
			Assert.IsFalse(database.Exists);
		}

		[Test]
		public void CreateNew() {
			database = new Database(databaseContext);
			database.Create(TestAdminUser, TestAdminPass);
		}

		[Test]
		public void AuthenticateAdmin() {
			Assert.DoesNotThrow(() => database.Authenticate(TestAdminUser, TestAdminPass));
		}
	}
}
