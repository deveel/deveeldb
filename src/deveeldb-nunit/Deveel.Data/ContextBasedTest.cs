// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
using System;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Services;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data {
	public abstract class ContextBasedTest {
		protected const string AdminUserName = "SA";
		protected const string AdminPassword = "1234567890";
		protected const string DatabaseName = "testdb";

		protected ContextBasedTest(StorageType storageType) {
			StorageType = storageType;
		}

		protected ContextBasedTest()
			: this(StorageType.InMemory) {
		}

		protected StorageType StorageType { get; private set; }

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
			return database.CreateUserSession(AdminUserName, AdminPassword);
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
			//if (!SingleContext)
			CreateContext();

			var testName = TestContext.CurrentContext.Test.Name;
			OnSetUp(testName);
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp() {
			System = CreateSystem();

			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", DatabaseName);

			if (StorageType == StorageType.InMemory) {
				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.Heap);
			} else if (StorageType == StorageType.JournaledFile) {
				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.Journaled);
				dbConfig.SetValue("database.path", Path.Combine(Environment.CurrentDirectory, DatabaseName));
			} else if (StorageType == StorageType.SingleFile) {
				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.SingleFile);
				dbConfig.SetValue("database.basePath", Environment.CurrentDirectory);
			}

			Database = CreateDatabase(System, dbConfig);

			OnFixtureSetUp();
		}

		private void CreateContext() {
			Session = CreateAdminSession(Database);
			Query = CreateQuery(Session);
		}

		private void DisposeContext() {
			if (Query != null)
				Query.Dispose();

			Query = null;
			Session = null;
		}

		[TearDown]
		public void TestTearDown() {
			OnTearDown();
			DisposeContext();
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown() {
			OnFixtureTearDown();

			if (Database != null)
				Database.Dispose();

			if (System != null)
				System.Dispose();

			if (StorageType == StorageType.JournaledFile) {
				var dataDir = Path.Combine(Environment.CurrentDirectory, DatabaseName);
				if (Directory.Exists(dataDir))
					Directory.Delete(dataDir, true);
			} else if (StorageType == StorageType.SingleFile) {
				var fileName = Path.Combine(Environment.CurrentDirectory, String.Format("{0}.db", DatabaseName));
				if (File.Exists(fileName))
					File.Delete(fileName);
			}
		}

		protected virtual void OnFixtureSetUp() {
			
		}

		protected virtual void OnFixtureTearDown() {
			
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
