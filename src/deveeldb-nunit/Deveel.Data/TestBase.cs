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

using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data {
	public abstract class TestBase {
		private readonly StorageType storageType;
		private DbSystem system;

		protected const string DatabaseName = "testdb";
		protected const string AdminUser = "SA";
		protected const string AdminPassword = "pass";

		private static int _connCounter = -1;

		private DeveelDbConnection connection;
		private DeveelDbTransaction transaction;

		protected TestBase(StorageType storageType) {
			this.storageType = storageType;
		}

		protected TestBase()
			: this(StorageType.Memory) {
		}

		protected DbSystem System {
			get { return system; }
		}

		protected DeveelDbConnection Connection {
			get { return connection; }
		}

		protected virtual bool RequiresSchema {
			get { return false; }
		}

		protected virtual void OnCreateTables() {
		}

		protected virtual void OnInsertData() {
		}

		[TestFixtureSetUp]
		public void SetUp() {
			DbConfig config = new DbConfig();
			OnConfigure(config);
			DbController controller = DbController.Create(config);

			system = !controller.DatabaseExists(DatabaseName)
						? controller.CreateDatabase(config, DatabaseName, AdminUser, AdminPassword)
						: controller.StartDatabase(config, DatabaseName);

			OnSetUp();
		}

		[TestFixtureTearDown]
		public void TearDown() {
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

		[SetUp]
		public void TestSetUp() {
			connection = (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
			connection.AutoCommit = false;

			if (RequiresSchema)
				GenerateDatabase();

			OnTestSetUp();
		}

		[TearDown]
		public void TestTearDown() {
			OnTestTearDown();

			if (RequiresSchema)
				DropTables();

			if (connection != null)
				connection.Close();
		}

		private void GenerateDatabase() {
			using (DeveelDbTransaction t = connection.BeginTransaction()) {
				try {
					GenerateTables();
					OnCreateTables();

					t.Commit();
				} catch (Exception) {
					throw;
				}
			}

			using (var t = connection.BeginTransaction()) {
				try {
					InsertDataPerson();
					InsertDataMusicGroup();
					InsertDataListensTo();
					OnInsertData();
					t.Commit();
				} catch (Exception) {
					throw;
				}
			}
		}

		private void GenerateTables() {
			DeveelDbCommand command = connection.CreateCommand("    CREATE TABLE IF NOT EXISTS Person ( " +
			                                                   "       id        IDENTITY, " +
			                                                   "       name      VARCHAR(100) NOT NULL, " +
			                                                   "       age       INTEGER, " +
			                                                   "       lives_in  VARCHAR(100) ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    CREATE TABLE IF NOT EXISTS ListensTo ( " +
			                                   "       id               IDENTITY, " +
			                                   "       person_name      VARCHAR(100) NOT NULL, " +
			                                   "       music_group_name VARCHAR(250) NOT NULL ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    CREATE TABLE IF NOT EXISTS MusicGroup ( " +
											   "       id                IDENTITY, " +
											   "       name              VARCHAR(250) NOT NULL, " +
											   "       country_of_origin VARCHAR(100) ) ");
			command.ExecuteNonQuery();
		}

		private void DropTables() {
			BeginTransaction();

			try {
				DeveelDbCommand command = connection.CreateCommand("DROP TABLE IF EXISTS MusicGroup");
				command.ExecuteNonQuery();

				command = connection.CreateCommand("DROP TABLE IF EXISTS ListensTo");
				command.ExecuteNonQuery();

				command = connection.CreateCommand("DROP TABLE IF EXISTS Person");
				command.ExecuteNonQuery();

				Commit();
			} catch (Exception e) {
				try {
					transaction.Rollback();
				} catch (Exception e2) {
					Console.Error.WriteLine("Was not able to rollback: {0}", e2.Message);
					Console.Error.WriteLine(e2.StackTrace);
				}

				throw e;
			}
		}

		private void InsertDataPerson() {
			DeveelDbCommand command;

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
														 "      ( 'Robert Bellamy', 24, 'England' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Grayham Downer', 59, 'Africa' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Timothy French', 24, 'Africa' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Butch Fad', 53, 'USA' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Judith Brown', 34, 'Africa' ) ");

			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Elizabeth Kramer', 24, 'USA' ) ");

			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Yamnik Wordsworth', 14, 'Australia' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Domonic Smith', 25, 'England' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Ivan Wilson', 23, 'England' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Lisa Williams', 24, 'England' ) ");

			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'Xenia, Warrior Princess', 32, 'Rome' ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
											   "      ( 'David Powell', 25, 'New Zealand' ) ");
			command.ExecuteNonQuery();
		}

		private void InsertDataMusicGroup() {
			DeveelDbCommand command = connection.CreateCommand("    INSERT INTO MusicGroup " +
			                                                   "      ( name, country_of_origin ) VALUES " +
			                                                   "      ( 'Oasis',       'England' ), " +
			                                                   "      ( 'Fatboy Slim', 'England' ), " +
			                                                   "      ( 'Metallica',   'USA' ), " +
			                                                   "      ( 'Nirvana',     'USA' ), " +
			                                                   "      ( 'Beatles',     'England' ), " +
			                                                   "      ( 'Fela Kuti',   'Africa' ), " +
			                                                   "      ( 'Blur',        'England' ), " +
			                                                   "      ( 'Muddy Ibe',   'Africa' ), " +
			                                                   "      ( 'Abba',        'Sweden' ), " +
			                                                   "      ( 'Madonna',     'USA' ), " +
			                                                   "      ( 'Cure',        'England' ) ");

			command.ExecuteNonQuery();
		}

		private void InsertDataListensTo() {
			DeveelDbCommand command = connection.CreateCommand("    INSERT INTO ListensTo " +
			                                                   "      ( person_name, music_group_name ) VALUES " +
			                                                   "      ( 'David Powell',             'Metallica' ), " +
			                                                   "      ( 'David Powell',             'Cure' ), " +
			                                                   "      ( 'Xenia, Warrior Princess',  'Madonna' ), " +
			                                                   "      ( 'Lisa Williams',            'Blur' ), " +
			                                                   "      ( 'Lisa Williams',            'Cure' ), " +
			                                                   "      ( 'Lisa Williams',            'Beatles' ), " +
			                                                   "      ( 'Ivan Wilson',              'Cure' ), " +
			                                                   "      ( 'Ivan Wilson',              'Beatles' ), " +
			                                                   "      ( 'Yamnik Wordsworth',        'Abba' ), " +
			                                                   "      ( 'Yamnik Wordsworth',        'Fatboy Slim' ), " +
			                                                   "      ( 'Yamnik Wordsworth',        'Fela Kuti' ), " +
			                                                   "      ( 'Elizabeth Kramer',         'Nirvana' ), " +
			                                                   "      ( 'Judith Brown',             'Fela Kuti' ), " +
			                                                   "      ( 'Judith Brown',             'Muddy Ibe' ), " +
			                                                   "      ( 'Butch Fad',                'Metallica' ), " +
			                                                   "      ( 'Timothy French',           'Blur' ), " +
			                                                   "      ( 'Timothy French',           'Oasis' ), " +
			                                                   "      ( 'Timothy French',           'Nirvana' ), " +
			                                                   "      ( 'Grayham Downer',           'Fela Kuti' ), " +
			                                                   "      ( 'Grayham Downer',           'Beatles' ), " +
			                                                   "      ( 'Robert Bellamy',           'Oasis' ), " +
			                                                   "      ( 'Robert Bellamy',           'Beatles' ), " +
			                                                   "      ( 'Robert Bellamy',           'Abba' ), " +
			                                                   "      ( 'Robert Bellamy',           'Blur' ) ");

			command.ExecuteNonQuery();
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

		protected void ExecuteNonQuery(string commandText) {
			DeveelDbCommand command = Connection.CreateCommand(commandText);
			command.ExecuteNonQuery();
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