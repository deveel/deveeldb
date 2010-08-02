using System;
using System.Reflection;

using Deveel.Data.Client;
using Deveel.Data.Control;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public abstract class TestBase {
		private DbSystem system;

		protected const string DatabaseName = "testdb";
		protected const string AdminUser = "SA";
		protected const string AdminPassword = "pass";

		private static int conn_counter = -1;

		private DeveelDbConnection connection;
		private bool generated;
		private bool generateOnce;

		protected DbSystem System {
			get { return system; }
		}

		protected DeveelDbConnection Connection {
			get { return connection; }
		}

		protected virtual void OnCreateTables(DeveelDbConnection connection) {
		}

		protected virtual void OnInsertData(DeveelDbConnection connection) {
		}

		[TestFixtureSetUp]
		public void SetUp() {
			DbController controller = DbController.Default;
			IDbConfig config = controller.Config;

			OnConfigure(config);

			system = !controller.DatabaseExists(DatabaseName)
						? controller.CreateDatabase(config, DatabaseName, AdminUser, AdminPassword)
						: controller.StartDatabase(config, DatabaseName);

			Attribute attribute = Attribute.GetCustomAttribute(GetType(), typeof(GenerateDatabaseAttribute), false);
			if (attribute == null || ((GenerateDatabaseAttribute)attribute).Generate) {
				generateOnce = true;
				GenerateDatabase();
			}

			OnSetUp();
		}

		[TestFixtureTearDown]
		public void TearDown() {
			OnTearDown();
			system.Close();
		}

		protected virtual void OnConfigure(IDbConfig config) {
			StorageBasedAttribute storageAttr = Attribute.GetCustomAttribute(GetType(), typeof(StorageBasedAttribute)) as StorageBasedAttribute;
			if (storageAttr != null) {
				if (storageAttr.Type == StorageType.File) {
					config.SetValue("storage_system", "v1file");
				} else if (storageAttr.Type == StorageType.Memory) {
					config.SetValue("storage_system", "v1heap");
				} else {
					config.SetValue("storage_system", storageAttr.CustomType);
				}
			} else {
				config.SetValue("storage_system", "v1heap");
			}
		}

		protected virtual void OnSetUp() {
		}

		protected virtual void OnTearDown() {
		}

		[SetUp]
		public virtual void TestSetUp() {
			if (!generateOnce && !generated)
				GenerateDatabase();
		}

		[TearDown]
		public virtual void TestTearDown() {
			if (generated && !generateOnce) {
				if (connection != null)
					connection.Dispose();
			}
		}

		private void GenerateDatabase() {
			if (!generated) {
				try {
					connection = CreateConnection();
					GenerateTables(connection);
					OnCreateTables(connection);

					InsertDataPerson(connection);
					InsertDataMusicGroup(connection);
					InsertDataListensTo(connection);
					OnInsertData(connection);

					generated = true;
				} finally {
					if (connection != null)
						connection.Dispose();
				}
			}
		}

		protected bool TablesGenerated(DeveelDbConnection connection) {
			DeveelDbCommand command = connection.CreateCommand("SHOW TABLES");
			DeveelDbDataReader reader = command.ExecuteReader();

			while (reader.Read()) {
				string tableName = reader.GetString(0);
				if (!tableName.Equals("Person") &&
					!tableName.Equals("ListensTo") &&
					!tableName.Equals("MusicGroup"))
					return false;
			}

			return true;
		}

		internal void GenerateTables(DeveelDbConnection connection) {
			DeveelDbCommand command = connection.CreateCommand("    CREATE TABLE Person ( " +
			                                                   "       id        IDENTITY, " +
			                                                   "       name      VARCHAR(100) NOT NULL, " +
			                                                   "       age       INTEGER, " +
			                                                   "       lives_in  VARCHAR(100) ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    CREATE TABLE ListensTo ( " +
			                                   "       id               IDENTITY, " +
			                                   "       person_name      VARCHAR(100) NOT NULL, " +
			                                   "       music_group_name VARCHAR(250) NOT NULL ) ");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("    CREATE TABLE MusicGroup ( " +
											   "       id                IDENTITY, " +
											   "       name              VARCHAR(250) NOT NULL, " +
											   "       country_of_origin VARCHAR(100) ) ");
			command.ExecuteNonQuery();
		}

		internal void InsertDataPerson(DeveelDbConnection connection) {
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

		internal void InsertDataMusicGroup(DeveelDbConnection connection) {
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

		internal void InsertDataListensTo(DeveelDbConnection connection) {
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

		protected DeveelDbConnection CreateConnection() {
			return (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
		}

		protected DatabaseConnection CreateDatabaseConnection() {
			string host_string = "Internal/Test/" + conn_counter++;
			User user = system.Database.AuthenticateUser(AdminUser, AdminPassword, host_string);
			return system.Database.CreateNewConnection(user, null);
		}
	}
}