using System;

using Deveel.Data.Client;
using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class SelectTest : SqlTestBase {
		protected override void OnTestSetUp() {
			Connection.AutoCommit = true;

			base.OnTestSetUp();
		}

		[Test(Description = "Declares and selects a gobal variable")]
		public void VariableSelect() {
			BeginTransaction();
			ExecuteNonQuery("DECLARE var NUMERIC(20) = 43");
			object var = ExecuteScalar("SELECT :var");

			Assert.IsNotNull(var);
			Assert.IsTrue(var.Equals(43));
		}

		[Test(Description = "Makes a simple plain select of all columns in a table")]
		public void SimpleSelectAll() {
			DeveelDbDataReader reader = ExecuteReader("SELECT * FROM Person");

			Assert.IsTrue(reader.HasRows);

			int rowCount = 0;
			while (reader.Read()) {
				rowCount++;
			}

			Assert.AreEqual(12, rowCount);
			reader.Close();
		}

		[Test(Description = "Selects three columns of two joined tables")]
		public void InnerJoinSelect() {
			DeveelDbDataReader reader =
				ExecuteReader("SELECT a.name AS GroupName, a.country_of_origin AS Country, b.person_name AS Person " +
				              "FROM MusicGroup AS a, ListensTo AS b WHERE a.name = b.music_group_name");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(3, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(24, rowCount);
			reader.Close();
		}

		[Test(Description = "Forms a result that is an outer join of two tables")]
		public void LeftOuterJoinSelect() {
			DeveelDbDataReader reader =
				ExecuteReader("SELECT a.name AS GroupName, a.country_of_origin AS Country, b.person_name AS Person " +
				              "FROM MusicGroup AS a LEFT OUTER JOIN ListensTo AS b ON a.name = b.music_group_name " +
				              "AND b.person_name = 'Robert Bellamy'");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(3, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(11, rowCount);
			reader.Close();
		}

		[Test]
		public void RightOuterJoin() {
			DeveelDbDataReader reader = ExecuteReader("SELECT a.name AS GroupName, a.country_of_origin AS Country, b.person_name AS Person " +
				  "FROM MusicGroup AS a RIGHT OUTER JOIN ListensTo AS b ON a.name = b.music_group_name " +
				  "AND b.person_name = 'Robert Bellamy'");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(3, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(24, rowCount);
			reader.Close();

		}

		[Test(Description = "Gets a set of columns from a group formed by another selection")]
		public void SubquerySelect() {
			DeveelDbDataReader reader = ExecuteReader("SELECT country_of_origin AS Country " +
			                                          "FROM (SELECT * FROM MusicGroup WHERE name = 'Oasis')");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(1, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(1, rowCount);
			reader.Close();
		}

		[Test(Description = "Stores a result of a selection with a single column into a simple variable")]
		public void SelectIntoSimpleVariable() {
			BeginTransaction();
			ExecuteNonQuery("DECLARE firstName VARCHAR(200)");
			ExecuteNonQuery("SELECT name INTO :firstName FROM Person WHERE name = 'Elizabeth Kramer'");
			object value = ExecuteScalar("SELECT :firstName");

			Assert.IsNotNull(value);
			Assert.IsTrue(value.Equals("Elizabeth Kramer"));
		}

		[Test(Description = "Stores a result of a complex selection into a variable with a complex type")]
		public void SelectIntoComplexVariable() {
			Assert.Inconclusive();
		}

		[Test(Description = "Stores the result of a selection into a table")]
		public void SelectIntoTable() {
			ExecuteNonQuery("CREATE TABLE FirstNames (name VARCHAR(100) NOT NULL)");
			ExecuteNonQuery("SELECT name INTO FirstNames FROM Person");

			DatabaseConnection connection = CreateDatabaseConnection();
			Table table1 = connection.GetTable("Person");
			Table table2 = connection.GetTable("FirstNames");

			Assert.IsNotNull(table1);
			Assert.IsNotNull(table2);

			Assert.AreEqual(table1.RowCount, table2.RowCount);
		}

		[Test(Description = "Selects all the values in a group that satisfy a given condition")]
		public void SelectIn() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name FROM Person WHERE lives_in IN ('England', 'Australia')");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(1, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(5, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectNotIn() {
			object value = ExecuteScalar("SELECT COUNT(*) FROM Person WHERE lives_in NOT IN ('Africa')");

			Assert.IsNotNull(value);
			Assert.IsTrue(value.Equals(9));
		}

		[Test]
		public void SelectAllEqual() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE age = ALL (24)");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(4, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectAllGreater() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE age > ALL (30)");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(4, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectAny() {
			Assert.Inconclusive();
		}

		[Test]
		public void UnionAll() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name FROM Person UNION ALL SELECT person_name FROM ListensTo");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(1, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);
			reader.Close();
		}

		[Test]
		public void SelectBetweenNumeric() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE age BETWEEN 10 AND 20");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(1, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectBetweenDates() {
			Assert.Inconclusive();
		}

		[Test]
		public void SelectBetweenStrings() {
			Assert.Inconclusive();
		}

		[Test]
		public void SelectGreaterThan() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE age > 30");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(4, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectLikeStart() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE name LIKE 'T%'");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(1, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectLikeEnd() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE name LIKE '%ess'");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(1, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectRegex() {
			Assert.Inconclusive();
		}

		[Test]
		public void SelectSmallerThan() {
			DeveelDbDataReader reader = ExecuteReader("SELECT name, age FROM Person WHERE age < 30");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(2, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(8, rowCount);
			reader.Close();
		}

		[Test(Description = "Outputs the result of a function on a selection")]
		public void FunctionSelect() {
			Assert.Inconclusive();
		}

		[Test]
		public void SelectCount() {
			object value = ExecuteScalar("SELECT COUNT(*) FROM Person WHERE name = 'Robert Bellamy'");
			Assert.IsNotNull(value);
			Assert.IsTrue(value.Equals(1));
		}

		[Test]
		public void DistinctSelect() {
			DeveelDbDataReader reader = ExecuteReader("SELECT DISTINCT lives_in FROM Person");

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(1, reader.FieldCount);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(6, rowCount);
			reader.Close();
		}

		[Test]
		public void GroupBySelect() {
			DeveelDbDataReader reader = ExecuteReader("SELECT country_of_origin FROM MusicGroup GROUP BY country_of_origin");

			Assert.IsTrue(reader.HasRows);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(4, rowCount);
			reader.Close();
		}

		[Test]
		public void HavingSelect() {
			DeveelDbDataReader reader = ExecuteReader("SELECT lives_in FROM Person GROUP BY lives_in HAVING AVG(age) > 30");

			Assert.IsTrue(reader.HasRows);

			int rowCount;
			PrintResult(reader, out rowCount);

			Assert.AreEqual(3, rowCount);
			reader.Close();
		}

		[Test]
		public void SelectIntoSingleVar() {
			DeveelDbConnection connection = Connection;
			connection.AutoCommit = false;

			DeveelDbCommand command = connection.CreateCommand("name VARCHAR(100) NOT NULL");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("SELECT :name");
			object value = command.ExecuteScalar();

			Assert.AreEqual(DBNull.Value, value);

			command = connection.CreateCommand("SELECT name INTO :name FROM Person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("SELECT name INTO :name FROM Person");

			command = connection.CreateCommand("SELECT :name");
			value = command.ExecuteScalar();

			Assert.IsNotNull(value);

			Console.Out.WriteLine("name = {0}", value);
		}

		[Test]
		public void SelectIntoTwoVars() {
			DeveelDbConnection connection = Connection;
			connection.AutoCommit = false;

			DeveelDbCommand command = connection.CreateCommand("name VARCHAR(100) NOT NULL");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("age INTEGER");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("SELECT :name");
			object value = command.ExecuteScalar();

			Assert.AreEqual(DBNull.Value, value);

			command = connection.CreateCommand("SELECT :age");
			value = command.ExecuteScalar();

			Assert.AreEqual(DBNull.Value, value);

			command = connection.CreateCommand("SELECT name, age INTO :name, :age FROM Person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("SELECT name, age INTO :name, :age FROM Person");

			command = connection.CreateCommand("SELECT :name");
			value = command.ExecuteScalar();

			Assert.IsNotNull(value);

			Console.Out.WriteLine("name = {0}", value);

			command = connection.CreateCommand("SELECT :age");
			value = command.ExecuteScalar();

			Assert.IsNotNull(value);

			Console.Out.WriteLine("age = {0}", value);
		}

		[Test]
		public void SelectSingleVariable() {
			Assert.Inconclusive();
		}
	}
}