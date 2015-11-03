using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class ContextBasedTest {
		protected const string AdminUserName = "SA";
		protected const string AdminPassword = "1234567890";
		protected const string DatabaseName = "testdb";

		protected virtual bool SingleContext {
			get { return false; }
		}

		protected IQueryContext QueryContext { get; private set; }

		protected ISystemContext SystemContext { get; private set; }

		protected IDatabase Database { get; private set; }

		protected IDatabaseContext DatabaseContext { get; private set; }

		protected virtual IDatabase CreateDatabase(IDatabaseContext context) {
			var database = new Database(context);
			database.Create(AdminUserName, AdminPassword);
			database.Open();

			return database;
		}

		protected virtual IQueryContext CreateQueryContext(IDatabase database) {
			return database.CreateQueryContext(AdminUserName, AdminPassword);
		}

		protected IQueryContext CreateUserQueryContext(string userName, string password) {
			return Database.CreateQueryContext(userName, password);
		}

		protected virtual void OnSetUp(string testName) {
			
		}

		protected virtual void OnTearDown() {
			
		}

		protected virtual IDatabaseContext CreateDatabaseContext(ISystemContext context) {
			return new DatabaseContext(context, DatabaseName);
		}

		protected virtual ISystemContext CreateSystemContext() {
			return new SystemContext();
		}
		
		[SetUp]
		public void TestSetUp() {
			if (!SingleContext)
				CreateContext();

			var testName = TestContext.CurrentContext.Test.Name;
			OnSetUp(testName);
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp() {
			if (SingleContext)
				CreateContext();
		}

		private void CreateContext() {
			SystemContext = CreateSystemContext();
			DatabaseContext = CreateDatabaseContext(SystemContext);
			Database = CreateDatabase(DatabaseContext);
			QueryContext = CreateQueryContext(Database);
		}

		private void DisposeContext() {
			if (QueryContext != null)
				QueryContext.Dispose();

			if (Database != null)
				Database.Dispose();

			if (DatabaseContext != null)
				DatabaseContext.Dispose();

			if (SystemContext != null)
				SystemContext.Dispose();

			Database = null;
			DatabaseContext = null;
			SystemContext = null;
			QueryContext = null;
		}

		[TearDown]
		public void TestTearDown() {
			OnTearDown();

			if (!SingleContext)
				DisposeContext();
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown() {
			if (SingleContext)
				DisposeContext();
		}
	}
}
