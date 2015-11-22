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

			if (testName != "OpenDatabase" &&
				testName != "CreateNewDatabase") {
				database.Open();
			}
		}

		[SetUp]
		public void SetUp() {
			var test = TestContext.CurrentContext.Test;

			OnSetUp(test.Name);
		}

		[TearDown]
		public void TearDown() {
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
