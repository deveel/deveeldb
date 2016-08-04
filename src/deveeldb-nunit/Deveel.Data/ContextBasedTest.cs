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
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Services;
using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data {
	public abstract class ContextBasedTest {
		protected const string AdminUserName = "SA";
		protected const string AdminPassword = "1234567890";
		protected const string DatabaseName = "testdb";
		protected const string UserName = "test_user";
		protected const string UserPassword = "abc12345";

		protected ContextBasedTest(StorageType storageType) {
			StorageType = storageType;
		}

		protected ContextBasedTest()
			: this(StorageType.InMemory) {
		}

		protected StorageType StorageType { get; private set; }

		protected IQuery AdminQuery { get; private set; }

		protected IQuery UserQuery { get; private set; }

		protected virtual bool CreateTestUser {
			get { return false; }
		}

		protected ISystem System { get; private set; }

		protected IDatabase Database { get; private set; }

		protected ISession AdminSession { get; private set; }

		protected ISession UserSession { get; private set; }

		private List<Exception> errors;

		protected IEnumerable<Exception> Errors {
			get { return errors == null ? new Exception[0] : errors.AsEnumerable(); }
		}

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
			return database.CreateSession(AdminUserName, AdminPassword);
		}

		protected virtual IQuery CreateQuery(ISession session) {
			return session.CreateQuery();
		}

		protected ISession CreateUserSession(string userName, string password) {
			return Database.CreateSession(userName, password);
		}

		protected virtual bool OnSetUp(string testName, IQuery query) {
			return false;
		}

		protected virtual void OnAfterSetup(string testName) {
		}

		protected virtual bool OnTearDown(string testName, IQuery query) {
			return false;

		}

		protected virtual void OnBeforeTearDown(string testName) {
			AssertNoErrors(testName);
		}

		[SetUp]
		public void TestSetUp() {
			//if (!SingleContext)

			errors = null;

			var testName = TestContext.CurrentContext.Test.Name;

			using (var session = CreateAdminSession(Database)) {
				using (var query = session.CreateQuery()) {
					if(OnSetUp(testName, query))
						query.Commit();
				}
			}

			CreateContext();

			OnAfterSetup(testName);
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp() {
			System = CreateSystem();
			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", DatabaseName);

#if PCL
			var dbPath = FileSystem.Local.CombinePath(".", DatabaseName);
#else
			var dbPath = Path.Combine(Environment.CurrentDirectory, DatabaseName);
#endif
			if (StorageType == StorageType.InMemory) {
				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.Heap);
			} else if (StorageType == StorageType.JournaledFile) {
				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.Journaled);
				dbConfig.SetValue("database.path", dbPath);
			} else if (StorageType == StorageType.SingleFile) {
				if (!FileSystem.Local.DirectoryExists(dbPath))
					FileSystem.Local.CreateDirectory(dbPath);

				dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.SingleFile);
				dbConfig.SetValue("database.basePath", dbPath);
			}

			DeleteFiles();

			Database = CreateDatabase(System, dbConfig);

			OnFixtureSetUp();
		}

		private void CreateContext() {
			if (CreateTestUser)
				CreateUserForTests();

			AdminSession = CreateAdminSession(Database);
			AdminSession.Context.RouteImmediate<ErrorEvent>(e => {
				if (errors == null)
					errors = new List<Exception>();

				if (e.Level != ErrorLevel.Warning)
					errors.Add(e.Error);
			});

			AdminQuery = CreateQuery(AdminSession);
		}

		private void CreateUserForTests() {
			using (var session = CreateAdminSession(Database)) {
				using (var query = CreateQuery(session)) {
					query.CreateUser(UserName, UserPassword);
					query.Commit();
				}
			}

			UserSession = CreateUserSession(UserName, UserPassword);
			UserQuery = CreateQuery(UserSession);
		}

		private void RemoveTesterUser() {
			using (var session = CreateAdminSession(Database)) {
				using (var query = session.CreateQuery()) {
					query.DropUser(UserName);
					query.Commit();
				}
			}
		}

		private void DisposeContext() {
			if (AdminQuery != null)
				AdminQuery.Dispose();
			if (UserQuery != null)
				UserQuery.Dispose();

			UserQuery = null;
			UserSession = null;
			AdminQuery = null;
			AdminSession = null;
		}

		protected virtual void AssertNoErrors(string testName) {
			if (errors != null && errors.Count > 0)
				throw new AggregateException(errors);
		}

		[TearDown]
		public void TestTearDown() {
			var testName = TestContext.CurrentContext.Test.Name;

			try {
				using (var session = CreateAdminSession(Database)) {
					using (var query = session.CreateQuery()) {
						if (OnTearDown(testName, query))
							query.Commit();
					}
				}

				OnBeforeTearDown(testName);
			} finally {
				if (errors != null)
					errors.Clear();

				errors = null;

				RemoveTesterUser();
				DisposeContext();
			}
		}

		private void DeleteFiles() {
			if (StorageType == StorageType.JournaledFile) {
#if PCL
				var dataDir = FileSystem.Local.CombinePath(".", DatabaseName);
				if (FileSystem.Local.DirectoryExists(dataDir))
					FileSystem.Local.CreateDirectory(dataDir);
#else
				var dataDir = Path.Combine(Environment.CurrentDirectory, DatabaseName);
				if (Directory.Exists(dataDir)) {
					Directory.Delete(dataDir, true);
				}
#endif
			} else if (StorageType == StorageType.SingleFile) {
#if PCL
				var fileName = FileSystem.Local.CombinePath(".", String.Format("{0}.db", DatabaseName));
				if (FileSystem.Local.FileExists(fileName))
					FileSystem.Local.DeleteFile(fileName);
#else
				var fileName = Path.Combine(Environment.CurrentDirectory, String.Format("{0}.db", DatabaseName));
				if (File.Exists(fileName))
					File.Delete(fileName);
#endif
			}
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown() {
			OnFixtureTearDown();

			if (Database != null) {
				Database.Close();
				Database.Dispose();
			}

			if (System != null)
				System.Dispose();

			DeleteFiles();

			GC.Collect(0, GCCollectionMode.Optimized);
			GC.Collect(1, GCCollectionMode.Forced);
			GC.Collect(2, GCCollectionMode.Forced);
			GC.Collect();
			GC.WaitForPendingFinalizers();
			var status = GC.WaitForFullGCComplete(-1);
			if (status == GCNotificationStatus.Timeout) {
				Console.Error.WriteLine("GC timed-out");
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
