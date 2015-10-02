using System;

using JetBrains.dotMemoryUnit;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[DotMemoryUnit(CollectAllocations = true)]
	public abstract class ContextBasedTest {
		private IUserSession session;

		protected const string AdminUserName = "SA";
		protected const string AdminPassword = "1234567890";
		protected const string DatabaseName = "testdb";

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
			session = database.CreateUserSession(AdminUserName, AdminPassword);
			return new SessionQueryContext(session);
		}

		protected virtual void OnSetUp() {
			
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
			SystemContext = CreateSystemContext();
			DatabaseContext = CreateDatabaseContext(SystemContext);
			Database = CreateDatabase(DatabaseContext);
			QueryContext = CreateQueryContext(Database);

			OnSetUp();
		}

		[TearDown]
		public void TestTearDown() {
			OnTearDown();

			if (QueryContext != null)
				QueryContext.Dispose();
			
			if (session != null)
				session.Dispose();

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
			session = null;
		}
	}
}
