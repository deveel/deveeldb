using System;

using Deveel.Data.Client;
using Deveel.Data.Control;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StorageType.Memory)]
	[TestFixture(StorageType.File)]
	public class CreateDatabaseTest {
		private readonly StorageType storageType;
		private DbSystem system;
		private DeveelDbConnection connection;
		private bool tablesCreated;

		private const string DatabaseName = "testdb";
		private const string AdminUser = "SA";
		private const string AdminPassword = "pass";

		public CreateDatabaseTest(StorageType storageType) {
			this.storageType = storageType;
		}

		private void OnConfigure(DbConfig config) {
			if (storageType == StorageType.File) {
				config.SetValue("storage_system", "v1file");
			} else if (storageType == StorageType.Memory) {
				config.SetValue("storage_system", "v1heap");
			}
		}


		[TestFixtureSetUp]
		public void SetUp() {
			DbConfig config = DbConfig.Default;
			OnConfigure(config);
			DbController controller = DbController.Create(Environment.CurrentDirectory, config);

			system = !controller.DatabaseExists(DatabaseName)
						? controller.CreateDatabase(config, DatabaseName, AdminUser, AdminPassword)
						: controller.StartDatabase(config, DatabaseName);

			connection = (DeveelDbConnection)system.GetConnection(AdminUser, AdminPassword);
			connection.AutoCommit = true;
		}

		[TestFixtureTearDown]
		public void TearDown() {
			if (connection != null)
				connection.Close();

			system.Close();
		}


		[Test]
		public void CreateTables() {
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
			tablesCreated = true;

			Assert.Pass();
		}

		[Test]
		public void InsertDataPerson() {
			CreateTables();

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

		[Test]
		public void InsertDataMusicGroup() {
			CreateTables();

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

		[Test]
		public void InsertDataListensTo() {
			CreateTables();

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

		[Test]
		public void FullCreate() {
			CreateTables();

			Console.Out.WriteLine("--- Inserting Data ---");

			InsertDataPerson();
			InsertDataMusicGroup();
			InsertDataListensTo();

			Console.Out.WriteLine("--- Complete ---");
		}

		[Test]
		public void OpenDatabase() {
			// we do nothing here cause the SetUp and TearDown methods
			// will open and close the connection...
		}
	}
}