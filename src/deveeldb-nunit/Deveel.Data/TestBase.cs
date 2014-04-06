// 
//  Copyright 2011 Deveel
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

using Deveel.Data.Client;
using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;
using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data {
	public abstract class TestBase {
		private readonly StorageType storageType;
		private Control.DbSystem system;

		private readonly List<Exception> errors;

		protected const string DatabaseName = "testdb";
		protected const string AdminUser = "SA";
		protected const string AdminPassword = "pass";

		private static int _connCounter = -1;

		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;

		protected TestBase(StorageType storageType) {
			this.storageType = storageType;

			errors = new List<Exception>();
		}

		protected TestBase()
			: this(StorageType.Memory) {
		}

		protected Control.DbSystem System {
			get { return system; }
		}

		protected DeveelDbConnection Connection {
			get { return connection; }
		}

		protected int ErrorCount {
			get { return errors.Count; }
		}

		protected bool HasErrors {
			get { return ErrorCount > 0; }
		}

		protected virtual void OnCreateTables() {
		}

		protected virtual void OnInsertData() {
		}

		[TestFixtureSetUp]
		public void FixtureSetUp() {
			DbConfig config = new DbConfig();
			OnConfigure(config);
			DbController controller = DbController.Create(config);


			system = !controller.DatabaseExists(DatabaseName)
						? controller.CreateDatabase(null, DatabaseName, AdminUser, AdminPassword)
						: controller.StartDatabase(null, DatabaseName);

			OnSetUp();
		}

		[TestFixtureTearDown]
		public void FixtureTearDown() {
			OnTearDown();

			if (transaction != null)
				transaction.Rollback();

			system.Close();
		}


		protected virtual void OnConfigure(DbConfig config) {
			if (storageType == StorageType.File) {
				config.SetValue(ConfigKeys.StorageSystem, ConfigValues.FileStorageSystem);
			} else if (storageType == StorageType.Memory) {
				config.SetValue(ConfigKeys.StorageSystem, ConfigValues.HeapStorageSystem);
			}
		}

		protected virtual void OnSetUp() {
		}

		protected virtual void OnTearDown() {
		}

		protected virtual void OnTestSetUp() {
		}

		protected virtual void OnTestTearDown() {	
		}

		protected void BeginTransaction() {
			if (transaction == null) {
				transaction = Connection.BeginTransaction();
			}
		}

		protected void Commit() {
			if (transaction != null) {
				transaction.Commit();
				transaction = null;
			}
		}

		protected void Rollback() {
			if (transaction != null) {
				transaction.Rollback();
				transaction = null;
			}
		}

		[SetUp]
		public void TestSetUp() {
			connection = (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
			connection.AutoCommit = false;
			OnTestSetUp();
		}

		[TearDown]
		public void TestTearDown() {
			OnTestTearDown();

			if (connection != null)
				connection.Close();
		}


		protected void PrintResult(DeveelDbDataReader reader) {
			int rowCount;
			PrintResult(reader, out rowCount);
		}

		protected void PrintResult(DeveelDbDataReader reader, out int rowCount) {
			rowCount = 0;

			while (reader.Read()) {
				for (int i = 0; i < reader.FieldCount; i++) {
					Console.Out.Write(reader.GetName(i));
					Console.Out.Write(" = ");
					Console.Out.Write(reader.GetValue(i));
					if (i < reader.FieldCount - 1)
						Console.Out.Write(", ");
				}

				rowCount++;
				Console.Out.WriteLine();
			}
		}

		protected DeveelDbDataReader ExecuteReader(string commandText) {
			DeveelDbCommand command = Connection.CreateCommand(commandText);
			return command.ExecuteReader();
		}

		protected int SafeExecuteNonQuery(string commandText) {
			try {
				return ExecuteNonQuery(commandText);
			} catch (Exception e) {
				errors.Add(e);
				return 0;
			}
		}

		protected int ExecuteNonQuery(string commandText) {
			DeveelDbCommand command = Connection.CreateCommand(commandText);
			return command.ExecuteNonQuery();
		}

		protected object ExecuteScalar(string commandText) {
			DeveelDbCommand command = Connection.CreateCommand(commandText);
			return command.ExecuteScalar();
		}

		protected DatabaseConnection CreateDatabaseConnection() {
			string hostString = "Internal/Test/" + _connCounter++;
			User user = system.Database.AuthenticateUser(AdminUser, AdminPassword, hostString);
			return system.Database.CreateNewConnection(user, null);
		}

		protected Table GetTable(string tableName) {
			DatabaseConnection dbConnection = CreateDatabaseConnection();
			return dbConnection.GetTable(tableName);
		}
	}
}