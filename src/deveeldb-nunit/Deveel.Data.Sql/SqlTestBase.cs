using System;

using Deveel.Data.Client;

namespace Deveel.Data.Sql {
	public abstract class SqlTestBase : TestBase {
		private void GenerateDatabase() {
			using (DeveelDbTransaction t = Connection.BeginTransaction()) {
				try {
					GenerateTables();
					t.Commit();
				} catch (Exception e) {
					Console.Error.WriteLine("An error occurred while generating the database structure");
					Console.Error.WriteLine(e.Message);
					Console.Error.WriteLine(e.StackTrace);
					throw;
				}
			}

			using (var t = Connection.BeginTransaction()) {
				try {
					InsertDataPerson();
					InsertDataMusicGroup();
					InsertDataListensTo();
					OnInsertData();
					t.Commit();
				} catch (Exception e) {
					Console.Error.WriteLine("An error occurred while inserting data");
					Console.Error.WriteLine(e.Message);
					Console.Error.WriteLine(e.StackTrace);
					throw;
				}
			}
		}

		private void GenerateTables() {
			ExecuteNonQuery(@"    CREATE TABLE IF NOT EXISTS Person ( " +
			                "       id        IDENTITY, " +
			                "       name      VARCHAR(100) NOT NULL, " +
			                "       age       INTEGER, " +
			                "       lives_in  VARCHAR(100) ) ", false);

			ExecuteNonQuery(@"    CREATE TABLE IF NOT EXISTS ListensTo ( " +
			                "       id               IDENTITY, " +
			                "       person_name      VARCHAR(100) NOT NULL, " +
			                "       music_group_name VARCHAR(250) NOT NULL ) ", false);

			ExecuteNonQuery(@"    CREATE TABLE IF NOT EXISTS MusicGroup ( " +
			                "       id                IDENTITY, " +
			                "       name              VARCHAR(250) NOT NULL, " +
			                "       country_of_origin VARCHAR(100) ) ", false);
		}

		private void InsertDataPerson() {
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Robert Bellamy', 24, 'England' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Grayham Downer', 59, 'Africa' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Timothy French', 24, 'Africa' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Butch Fad', 53, 'USA' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Judith Brown', 34, 'Africa' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Elizabeth Kramer', 24, 'USA' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Yamnik Wordsworth', 14, 'Australia' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Domonic Smith', 25, 'England' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Ivan Wilson', 23, 'England' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Lisa Williams', 24, 'England' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'Xenia, Warrior Princess', 32, 'Rome' ) ", false);
			ExecuteNonQuery("INSERT INTO Person ( name, age, lives_in ) VALUES ( 'David Powell', 25, 'New Zealand' ) ", false);
		}

		private void InsertDataMusicGroup() {
			ExecuteNonQuery(@"    INSERT INTO MusicGroup " +
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
			                "      ( 'Cure',        'England' ) ", false);

		}

		private void InsertDataListensTo() {
			ExecuteNonQuery("    INSERT INTO ListensTo " +
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
			                "      ( 'Robert Bellamy',           'Blur' ) ", false);

		}

		private void DropTables() {
			BeginTransaction();

			try {
				ExecuteNonQuery("DROP TABLE IF EXISTS MusicGroup", false);
				ExecuteNonQuery("DROP TABLE IF EXISTS ListensTo", false);
				ExecuteNonQuery("DROP TABLE IF EXISTS Person", false);
				Commit();
			} catch (Exception) {
				try {
					Rollback();
				} catch (Exception e2) {
					Console.Error.WriteLine("Was not able to rollback: {0}", e2.Message);
					Console.Error.WriteLine(e2.StackTrace);
				}

				throw;
			}
		}


		protected override void OnTestSetUp() {
			GenerateDatabase();
		}

		protected override void OnTestTearDown() {
			DropTables();
		}
	}
}