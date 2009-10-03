using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateDatabaseTest : TestBase {
		[Test]
		public void CreateTables() {
			Console.Out.WriteLine("--- Creating Tables ---");
			DbConnection connection = CreateConnection();

			DbCommand command = connection.CreateCommand();

			command.ExecuteNonQuery("    CREATE TABLE Person ( " +
			                        "       name      VARCHAR(100) NOT NULL, " +
			                        "       age       INTEGER, " +
			                        "       lives_in  VARCHAR(100) ) ");
			command.ExecuteNonQuery("    CREATE TABLE ListensTo ( " +
			                        "       person_name      VARCHAR(100) NOT NULL, " +
			                        "       music_group_name VARCHAR(250) NOT NULL ) ");
			command.ExecuteNonQuery("    CREATE TABLE MusicGroup ( " +
			                        "       name              VARCHAR(250) NOT NULL, " +
			                        "       country_of_origin VARCHAR(100) ) ");
			connection.Close();
		}

		[Test]
		public void InsertDataPerson() {
			DbConnection connection = CreateConnection();
			DbCommand command = connection.CreateCommand();

			Console.Out.WriteLine("-- Adding to Person Table --");

			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Robert Bellamy', 24, 'England' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Grayham Downer', 59, 'Africa' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Timothy French', 24, 'Africa' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Butch Fad', 53, 'USA' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Judith Brown', 34, 'Africa' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Elizabeth Kramer', 24, 'USA' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Yamnik Wordsworth', 14, 'Australia' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Domonic Smith', 25, 'England' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Ivan Wilson', 23, 'England' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Lisa Williams', 24, 'England' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'Xenia, Warrior Princess', 32, 'Rome' ) ");
			command.ExecuteNonQuery("    INSERT INTO Person ( name, age, lives_in ) VALUES " +
			                        "      ( 'David Powell', 25, 'New Zealand' ) ");
			connection.Close();
		}

		[Test]
		public void InsertDataMusicGroup() {
			DbConnection connection = CreateConnection();

			DbCommand command = connection.CreateCommand();

			Console.Out.WriteLine("-- Adding to MusicGroup Table --");

			command.ExecuteNonQuery("    INSERT INTO MusicGroup " +
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
			connection.Close();
		}

		[Test]
		public void InsertDataListensTo() {
			DbConnection connection = CreateConnection();

			DbCommand command = connection.CreateCommand();

			Console.Out.WriteLine("-- Adding to ListensTo Table --");

			command.ExecuteNonQuery("    INSERT INTO ListensTo " +
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
	}
}