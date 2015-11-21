using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class CreateTableStatementTests : ContextBasedTest {
		[Test]
		public void ParseSimpleCreate() {
			const string sql = "CREATE TABLE test (id INT, name VARCHAR)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;
			Assert.AreEqual(2, createTable.Columns.Count);

			var columns = createTable.Columns;

			Assert.AreEqual("id", columns[0].ColumnName);
			Assert.AreEqual(SqlTypeCode.Integer, columns[0].ColumnType.TypeCode);

			Assert.AreEqual("name", columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(columns[1].ColumnType);
		}

		[Test]
		public void SimpleCreate() {
			const string sql = "CREATE TABLE test (id INT, name VARCHAR)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WithColumnDefault() {
			const string sql = "CREATE TABLE test (id INT, name VARCHAR DEFAULT (67 * 90)+22, date TIMESTAMP DEFAULT GetDate())";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void ParseWithColumnDefault() {
			const string sql = "CREATE TABLE test (id INT, name VARCHAR DEFAULT (67 * 90)+22, date TIMESTAMP DEFAULT GetDate())";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement)statement;
			Assert.AreEqual(3, createTable.Columns.Count);

			var columns = createTable.Columns;

			Assert.AreEqual("id", columns[0].ColumnName);
			Assert.AreEqual(SqlTypeCode.Integer, columns[0].ColumnType.TypeCode);

			Assert.AreEqual("name", columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(columns[1].ColumnType);
			Assert.IsTrue(columns[1].HasDefaultExpression);
			Assert.IsInstanceOf<SqlBinaryExpression>(columns[1].DefaultExpression);

			Assert.AreEqual("date", columns[2].ColumnName);
			Assert.IsInstanceOf<DateType>(columns[2].ColumnType);
			Assert.IsTrue(columns[1].HasDefaultExpression);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(columns[2].DefaultExpression);
		}

		[Test]
		public void ParseWithColumnIdentity() {
			const string sql = "CREATE TABLE test (id INT IDENTITY, name VARCHAR NOT NULL)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement)statement;
			Assert.AreEqual(2, createTable.Columns.Count);

			var columns = createTable.Columns;

			Assert.AreEqual("id", columns[0].ColumnName);
			Assert.AreEqual(SqlTypeCode.Integer, columns[0].ColumnType.TypeCode);
			Assert.IsNotNull(columns[0].DefaultExpression);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(columns[0].DefaultExpression);

			Assert.AreEqual("name", columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(columns[1].ColumnType);
			Assert.IsTrue(columns[1].IsNotNull);
		}

		[Test]
		public void ParseWithColumnConstraints() {
			const string sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR NOT NULL)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(2, list.Count);
		}

		[Test]
		public void ParseWithColumnAndTableConstraints() {
			const string sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR NOT NULL, CONSTRAINT uk_test UNIQUE(name))";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(3, list.Count);
		}
	}
}
