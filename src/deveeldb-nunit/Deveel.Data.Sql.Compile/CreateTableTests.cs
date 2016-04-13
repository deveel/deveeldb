using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class CreateTableTests : SqlCompileTestBase {
		[Test]
		public void SimpleTable() {
			const string sql = "CREATE TABLE test (a INT NOT NULL, b VARCHAR NULL)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = (CreateTableStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;

			Assert.IsNotNull(createTable.TableName);
			Assert.AreEqual("test", createTable.TableName.Name);
			Assert.IsFalse(createTable.Temporary);
			Assert.IsFalse(createTable.IfNotExists);

			Assert.AreEqual(2, createTable.Columns.Count);
			Assert.AreEqual("a", createTable.Columns[0].ColumnName);
			Assert.IsTrue(createTable.Columns[0].IsNotNull);

			Assert.AreEqual("b", createTable.Columns[1].ColumnName);
			Assert.IsFalse(createTable.Columns[1].IsNotNull);
		}

		[Test]
		public void WithIdentityColumn() {
			const string sql = "CREATE TABLE test (id INT IDENTITY, name VARCHAR NOT NULL)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = (CreateTableStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;

			Assert.IsNotNull(createTable.TableName);
			Assert.AreEqual("test", createTable.TableName.Name);
			Assert.IsFalse(createTable.Temporary);
			Assert.IsFalse(createTable.IfNotExists);

			Assert.AreEqual(2, createTable.Columns.Count);
			Assert.IsTrue(createTable.Columns[0].IsIdentity);
			Assert.IsNull(createTable.Columns[0].DefaultExpression);
		}

		[Test]
		public void WithColumnDefault() {
			const string sql = "CREATE TABLE test (a VARCHAR DEFAULT 'one')";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = (CreateTableStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;

			Assert.IsNotNull(createTable.TableName);
			Assert.AreEqual("test", createTable.TableName.Name);

			Assert.AreEqual(1, createTable.Columns.Count);
			Assert.IsNotNull(createTable.Columns[0].DefaultExpression);
			Assert.IsInstanceOf<SqlConstantExpression>(createTable.Columns[0].DefaultExpression);
		}

		[Test]
		public void WithColumnDefault_Advanced() {
			const string sql =
				"CREATE TABLE test (id INT, name VARCHAR DEFAULT (67 * 90) + 22, date TIMESTAMP DEFAULT GetDate())";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;
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
		public void WithConstraints() {
			const string sql = "CREATE TABLE test (id INT NOT NULL, UNIQUE_IX UNIQUE(id))";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(2, result.Statements.Count);

			Assert.IsInstanceOf<CreateTableStatement>(result.Statements.ElementAt(0));
			Assert.IsInstanceOf<AlterTableStatement>(result.Statements.ElementAt(1));

			var statement = (CreateTableStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement) statement;

			Assert.IsNotNull(createTable.TableName);
			Assert.AreEqual("test", createTable.TableName.Name);
		}

		[Test]
		public void WithColumnConstraints() {
			const string sql = "CREATE TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL)";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(2, result.Statements.Count);

			Assert.IsInstanceOf<CreateTableStatement>(result.Statements.ElementAt(0));
			Assert.IsInstanceOf<AlterTableStatement>(result.Statements.ElementAt(1));

			var statement = (CreateTableStatement) result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			Assert.IsNotNull(statement.TableName);
			Assert.AreEqual("test", statement.TableName.Name);
		}

		[Test]
		public void WithColumnAndTableConstraints() {
			const string sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR NOT NULL, CONSTRAINT uk_test UNIQUE(name))";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			Assert.AreEqual(3, result.Statements.Count);

			Assert.IsInstanceOf<CreateTableStatement>(result.Statements.ElementAt(0));
			Assert.IsInstanceOf<AlterTableStatement>(result.Statements.ElementAt(1));
			Assert.IsInstanceOf<AlterTableStatement>(result.Statements.ElementAt(2));
		}

	}
}