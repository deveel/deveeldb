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

using NUnit.Framework;

namespace Deveel.Data.Store {
	[TestFixture]
	public class SingleFileStoreTests {
		private ISystem systemContext;
		private IDatabase database;

		private const string DatabaseName = "test_db";
		private const string TestAdminUser = "SA";
		private const string TestAdminPass = "abc1234";

		private static IConfiguration CreateDatabaseConfig() {
			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", DatabaseName);
			dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.SingleFile);
			dbConfig.SetValue("database.basePath", Environment.CurrentDirectory);
			return dbConfig;
		}

		private static string GetDbFileName() {
			var localPath = Environment.CurrentDirectory;
			var fileName = String.Format("{0}.db", DatabaseName);
            return Path.Combine(localPath, fileName);
		}

		private void OnSetUp(string testName) {
			var systemBuilder = new SystemBuilder();
			systemContext = systemBuilder.BuildSystem();

			var dbConfig = CreateDatabaseConfig();

			if (testName != "CreateNewDatabase") {
				database = systemContext.CreateDatabase(dbConfig, TestAdminUser, TestAdminPass); 
			}
		}

		[SetUp]
		public void SetUp() {
			var test = TestContext.CurrentContext.Test;

			OnSetUp(test.Name);
		}

		[TearDown]
		public void TearDown() {
			if (database != null) {
				database.Close();
				database.Dispose();
			}

			var fileName = GetDbFileName();
			if (File.Exists(fileName))
				File.Delete(fileName);
		}

		[Test]
		public void CreateNewDatabase() {
			var dbConfig = CreateDatabaseConfig();
			database = systemContext.CreateDatabase(dbConfig, TestAdminUser, TestAdminPass);
		}
	}
}
