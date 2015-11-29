using System;

using Deveel.Data.Configuration;
using Deveel.Data.Services;
using Deveel.Data.Transactions;

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

		protected IQuery Query { get; private set; }

		protected ISystem System { get; private set; }

		protected IDatabase Database { get; private set; }

		protected ISession Session { get; private set; }

		protected virtual void RegisterServices(ServiceContainer container) {
		}

		protected virtual ISystem CreateSystem() {
			var builder = new TestSystemBuilder(this);
			return builder.BuildSystem();
		}

		protected virtual IDatabase CreateDatabase(ISystem system, IConfiguration configuration) {
			return system.CreateDatabase(configuration, AdminUserName, AdminPassword);
		}

		protected virtual ISession CreateAdminSession(IDatabase database) {
			var user = database.Authenticate(AdminUserName, AdminPassword);
			var transaction = database.CreateTransaction(IsolationLevel.Serializable);
			return new Session(transaction, user);
		}

		protected virtual IQuery CreateQuery(ISession session) {
			return session.CreateQuery();
		}

		protected ISession CreateUserSession(string userName, string password) {
			return Database.CreateUserSession(userName, password);
		}

		protected virtual void OnSetUp(string testName) {
			
		}

		protected virtual void OnTearDown() {

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
			System = CreateSystem();

			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", DatabaseName);

			Database = CreateDatabase(System, dbConfig);
			Session = CreateAdminSession(Database);
			Query = CreateQuery(Session);
		}

		private void DisposeContext() {
			if (Query != null)
				Query.Dispose();

			if (Database != null)
				Database.Dispose();

			if (System != null)
				System.Dispose();

			Query = null;
			Database = null;
			Database = null;
			System = null;
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

		private class TestSystemBuilder : SystemBuilder {
			private ContextBasedTest test;

			public TestSystemBuilder(ContextBasedTest test) {
				this.test = test;
			}

			protected override void OnServiceRegistration(ServiceContainer container) {
				test.RegisterServices(container);
			}
		}
	}
}
