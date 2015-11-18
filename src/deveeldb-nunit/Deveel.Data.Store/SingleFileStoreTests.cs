using System;
using System.IO;

using Deveel.Data.Configuration;

using NUnit.Framework;

namespace Deveel.Data.Store {
	[TestFixture]
	public class SingleFileStoreTests {
		private ISystemContext systemContext;
		private IDatabaseContext databaseContext;
		private IDatabase database;

		private const string DatabaseName = "test_db";
		private const string TestAdminUser = "SA";
		private const string TestAdminPass = "abc1234";

		private static IDatabaseContext CreateDatabaseContext(ISystemContext context) {
			var config = context.Configuration;
			var dbConfig = new Configuration.Configuration(config);
			dbConfig.SetValue("database.name", DatabaseName);
			dbConfig.SetValue("database.storageSystem", DefaultStorageSystemNames.SingleFile);
			dbConfig.SetValue("database.basePath", Environment.CurrentDirectory);
			return new DatabaseContext(context, dbConfig);
		}

		private static string GetDbFileName() {
			var localPath = Environment.CurrentDirectory;
			var fileName = String.Format("{0}.db", DatabaseName);
            return Path.Combine(localPath, fileName);
		}

		private void OnSetUp(string testName) {
			systemContext = new SystemContext();
			databaseContext = CreateDatabaseContext(systemContext);

			if (testName != "CreateNewDatabase") {
				database = new Database(databaseContext);
				database.Create(TestAdminUser, TestAdminPass);
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
			database = new Database(databaseContext);
			database.Create(TestAdminUser, TestAdminPass);
		}
	}
}
