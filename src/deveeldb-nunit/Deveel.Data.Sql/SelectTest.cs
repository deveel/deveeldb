using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class SelectTest : TestBase {
		protected override bool RequiresSchema {
			get { return true; }
		}

		[Test(Description = "Declares and selects a gobal variable")]
		public void VariableSelect() {
			DeveelDbCommand command = Connection.CreateCommand("DECLARE var NUMERIC(20) = 43");
			command.ExecuteNonQuery();

			command = Connection.CreateCommand("SELECT :var");
			object var = command.ExecuteScalar();

			Assert.IsNotNull(var);
			Assert.IsTrue(var.Equals(43));
		}

		[Test(Description = "Makes a simple plain select of all columns in a table")]
		public void SimpleSelectAll() {
			DeveelDbCommand command = Connection.CreateCommand("SELECT * FROM Person");
			DeveelDbDataReader reader = command.ExecuteReader();

			Assert.IsTrue(reader.HasRows);

			int rowCount = 0;
			while (reader.Read()) {
				rowCount++;
			}

			Assert.AreEqual(12, rowCount);
		}

		[Test(Description = "Selects all columns of two joined tables")]
		public void InnerJoinSelect() {
			DeveelDbCommand command =
				Connection.CreateCommand("SELECT a.*, b.* FROM MusicGroup AS a, ListensTo AS b WHERE a.name = b.music_group_name");
			DeveelDbDataReader reader = command.ExecuteReader();

			Assert.IsTrue(reader.HasRows);
			Assert.AreEqual(6, reader.FieldCount);
			while (reader.Read()) {
				for (int i = 0; i < reader.FieldCount; i++) {
					Console.Out.Write(reader.GetName(i));
					Console.Out.Write(" = ");
					Console.Out.Write(reader.GetValue(i));
					if (i < reader.FieldCount - 1)
						Console.Out.Write(", ");
				}

				Console.Out.WriteLine();
			}
		}

		[Test(Description = "Forms a result that is an outer join of two tables")]
		public void LeftOuterJoinSelect() {

		}

		[Test]
		public void RightOuterJoin() {

		}

		[Test]
		public void FullOuterJoin() {

		}

		[Test(Description = "Gets a set of columns from a group formed by another selection")]
		public void SubquerySelect() {

		}

		[Test(Description = "Stores a result of a selection into a variable")]
		public void SelectIntoVariable() {

		}

		[Test(Description = "Stores the result of a selection into a table")]
		public void SelectIntoTable() {

		}

		[Test(Description = "Selects all the values in a group that satisfy a given condition")]
		public void SelectAllIn() {

		}

		[Test]
		public void SelectAllNotIn() {

		}

		[Test]
		public void SelectBetweenNumeric() {

		}

		[Test]
		public void SelectBetweenDates() {

		}

		[Test]
		public void SelectBetweenStrings() {

		}

		[Test]
		public void SelectGreaterThan() {

		}

		[Test]
		public void SelectLike() {

		}

		[Test]
		public void SelectSmallerThan() {
		}

		[Test(Description = "Outputs the result of a function on a selection")]
		public void FunctionSelect() {

		}

		[Test]
		public void SelectCount() {

		}

		[Test]
		public void DistinctSelect() {

		}

		[Test]
		public void GroupBySelect() {

		}

		[Test]
		public void HavingSelect() {

		}

		[Test]
		public void SelectIntoSingleVar() {
			DeveelDbConnection connection = Connection;
			connection.AutoCommit = false;

			DeveelDbCommand command = connection.CreateCommand("name VARCHAR(100) NOT NULL");
			command.ExecuteNonQuery();

			command = connection.CreateCommand("SELECT :name");
			object value = command.ExecuteScalar();

			Console.Out.WriteLine("name = {0}", value);

			command = connection.CreateCommand("SELECT name INTO :name FROM Person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("SELECT name INTO :name FROM Person");

			command = connection.CreateCommand("SELECT :name");
			value = command.ExecuteScalar();

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

			Console.Out.WriteLine("name = {0}", value);

			command = connection.CreateCommand("SELECT :age");
			value = command.ExecuteScalar();

			Console.Out.WriteLine("age = {0}", value);

			command = connection.CreateCommand("SELECT name, age INTO :name, :age FROM Person");
			command.ExecuteNonQuery();

			Console.Out.WriteLine("SELECT name, age INTO :name, :age FROM Person");

			command = connection.CreateCommand("SELECT :name");
			value = command.ExecuteScalar();

			Console.Out.WriteLine("name = {0}", value);

			command = connection.CreateCommand("SELECT :age");
			value = command.ExecuteScalar();

			Console.Out.WriteLine("age = {0}", value);
		}
	}
}