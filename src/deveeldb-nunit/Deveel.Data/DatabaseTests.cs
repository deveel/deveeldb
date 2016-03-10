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

		[Test]
		public void SimpleCreate() {
			var db = Database.New(TestDbName, TestAdminUser, TestAdminPass);

			Assert.IsNotNull(db);
			Assert.IsTrue(db.Exists);
		}
	}
}
