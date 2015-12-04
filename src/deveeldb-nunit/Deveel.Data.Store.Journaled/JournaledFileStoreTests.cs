using System;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Sql.Compile;

using NUnit.Framework;

namespace Deveel.Data.Store.Journaled {
	[TestFixture]
	public class JournaledFileStoreTests : SqlCompileTestBase {
		private ISystem systemContext;
		private IDatabase database;

		private const string DatabaseName = "test_db";
		private const string TestAdminUser = "SA";
		private const string TestAdminPass = "abc1234";

		private static IConfiguration CreateDatabaseConfig() {
			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", DatabaseName);
			dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.Journaled);
			dbConfig.SetValue("database.basePath", Environment.CurrentDirectory);
			return dbConfig;
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

			var dirName = Path.Combine(Environment.CurrentDirectory, DatabaseName);
			if (Directory.Exists(dirName))
				Directory.Delete(dirName);
		}

		//[Test]
		//public void CreateNewDatabase() {
		//	var dbConfig = CreateDatabaseConfig();
		//	database = systemContext.CreateDatabase(dbConfig, TestAdminUser, TestAdminPass);
		//}
	}
}
