using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class InsertIntoTests : SchemaCompileTests {
		[Test]
		public void ValuesInsert_OneRow() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) " +
			                   "VALUES ('Antonello', 'Provenzano', TOODATE('1980-06-04'))";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());
		}

		[Test]
		public void ValueInsert_MultipleRows() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) " +
			                   "VALUES ('Antonello', 'Provenzano', TODATE('1980-06-04')), " +
			                   "('Sebastiano', 'Provenzano', TODATE('1981-08-27'))";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(2, insertStatement.Values.Count());
		}

		[Test]
		public void SetInsert() {
			const string sql =
				"INSERT INTO test_table SET first_name = 'Antonello', last_name = 'Provenzano', birth_date = TODATE('1980-06-04')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());
		}

		[Test]
		public void InsertSelect() {
			const string sql = "INSERT INTO test_table FROM (SELECT * FROM table2 WHERE arg1 = 1)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertSelectStatement>(statement);
		}
	}
}