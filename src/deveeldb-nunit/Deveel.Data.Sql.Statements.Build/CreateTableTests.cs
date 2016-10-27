using System;
using System.Linq;

using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements.Build {
	[TestFixture]
	public class CreateTableTests {
		[Test]
		public void SimpleCreate() {
			var statements = SqlStatementBuilder.CreateTable(table => table
				.Named("app.test")
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String()));

			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(1, statements.Count());
			Assert.IsInstanceOf<CreateTableStatement>(statements.ElementAt(0));

			var createTable = (CreateTableStatement) statements.ElementAt(0);
			Assert.AreEqual(ObjectName.Parse("app.test"), createTable.TableName);
			Assert.IsNotEmpty(createTable.Columns);
			Assert.AreEqual(2, createTable.Columns.Count);

			Assert.AreEqual("id", createTable.Columns[0].ColumnName);
			Assert.IsInstanceOf<NumericType>(createTable.Columns[0].ColumnType);
			Assert.IsFalse(createTable.Columns[0].IsNotNull);

			Assert.AreEqual("name", createTable.Columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(createTable.Columns[1].ColumnType);
			Assert.IsFalse(createTable.Columns[1].IsNotNull);
		}

		[Test]
		public void WithIdentityColumn() {
			var statements = SqlStatementBuilder.CreateTable(table => table
				.Named("app.test")
				.WithIdentityColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String()));

			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(2, statements.Count());
			Assert.IsInstanceOf<CreateTableStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<AlterTableStatement>(statements.ElementAt(1));

			var createTable = (CreateTableStatement) statements.ElementAt(0);
			Assert.AreEqual(ObjectName.Parse("app.test"), createTable.TableName);
			Assert.IsNotEmpty(createTable.Columns);
			Assert.AreEqual(2, createTable.Columns.Count);

			Assert.AreEqual("id", createTable.Columns[0].ColumnName);
			Assert.IsInstanceOf<NumericType>(createTable.Columns[0].ColumnType);
			Assert.IsTrue(createTable.Columns[0].IsNotNull);

			Assert.AreEqual("name", createTable.Columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(createTable.Columns[1].ColumnType);
			Assert.IsFalse(createTable.Columns[1].IsNotNull);

			var alterTable = (AlterTableStatement) statements.ElementAt(1);
			Assert.AreEqual(ObjectName.Parse("app.test"), alterTable.TableName);
			Assert.IsInstanceOf<AddConstraintAction>(alterTable.Action);
		}

		[Test]
		public void OnlyIfExists() {
			var statements = SqlStatementBuilder.CreateTable(table => table
				.Named("app.test")
				.IfNotExists()
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String()));

			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(1, statements.Count());
			Assert.IsInstanceOf<CreateTableStatement>(statements.ElementAt(0));

			var createTable = (CreateTableStatement) statements.ElementAt(0);
			Assert.AreEqual(ObjectName.Parse("app.test"), createTable.TableName);
			Assert.IsTrue(createTable.IfNotExists);
			Assert.IsNotEmpty(createTable.Columns);
			Assert.AreEqual(2, createTable.Columns.Count);

			Assert.AreEqual("id", createTable.Columns[0].ColumnName);
			Assert.IsInstanceOf<NumericType>(createTable.Columns[0].ColumnType);
			Assert.IsFalse(createTable.Columns[0].IsNotNull);

			Assert.AreEqual("name", createTable.Columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(createTable.Columns[1].ColumnType);
			Assert.IsFalse(createTable.Columns[1].IsNotNull);
		}

		[Test]
		public void Temporary() {
			var statements = SqlStatementBuilder.CreateTable(table => table
				.Named("app.test")
				.Temporary()
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String()));

			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(1, statements.Count());
			Assert.IsInstanceOf<CreateTableStatement>(statements.ElementAt(0));

			var createTable = (CreateTableStatement) statements.ElementAt(0);
			Assert.AreEqual(ObjectName.Parse("app.test"), createTable.TableName);
			Assert.IsTrue(createTable.Temporary);
			Assert.IsNotEmpty(createTable.Columns);
			Assert.AreEqual(2, createTable.Columns.Count);

			Assert.AreEqual("id", createTable.Columns[0].ColumnName);
			Assert.IsInstanceOf<NumericType>(createTable.Columns[0].ColumnType);
			Assert.IsFalse(createTable.Columns[0].IsNotNull);

			Assert.AreEqual("name", createTable.Columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(createTable.Columns[1].ColumnType);
			Assert.IsFalse(createTable.Columns[1].IsNotNull);
		}
	}
}
