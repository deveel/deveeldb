using System;

using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Deveel.Data.DbSystem {
	[TestFixture]
	public abstract class ContextBasedTest {
		protected const string AdminUserName = "SA";
		protected const string AdminPassword = "1234567890";
		protected const string DatabaseName = "testdb";

		protected IQueryContext Context { get; private set; }

		protected ISystemContext SystemContext { get; private set; }

		protected IDatabase Database { get; private set; }

		protected IDatabaseContext DatabaseContext { get; private set; }

		protected virtual IDatabase CreateDatabase(IDatabaseContext context) {
			var database = new Database(context);
			database.Create(AdminUserName, AdminPassword);
			database.Open();

			return database;
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

			OnSetUp();
		}

		[TearDown]
		public void TestTearDown() {
			OnTearDown();

			if (Database != null)
				Database.Dispose();

			if (DatabaseContext != null)
				DatabaseContext.Dispose();

			if (SystemContext != null)
				SystemContext.Dispose();

			Database = null;
			DatabaseContext = null;
			SystemContext = null;
		}
	}
}
