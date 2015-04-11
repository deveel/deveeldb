using System;

using Deveel.Data.Configuration;
using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data.DbSystem {
	[TestFixture]
	public sealed class DatabaseTests {
		private ISystemContext systemContext;
		private IDatabaseContext databaseContext;

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
			var database = new Database(databaseContext);
			Assert.IsFalse(database.Exists);
		}

		[Test]
		public void CreateNew() {
			var database = new Database(databaseContext);
			Assert.DoesNotThrow(() => database.Create(TestAdminUser, TestAdminPass));
		}
	}
}
