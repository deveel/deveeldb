using System;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class AlterTableTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Access().CreateTable(tableInfo);
			query.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			query.Access().CreateTable(tableInfo);

			if (testName == "DropConstraint") {
				query.Session.Access().AddForeignKey(tableInfo.TableName, new string[] { "person_id" }, ObjectName.Parse("APP.test_table"),
					new[] { "id" }, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, "FK_1");
			}
		}

		protected override void OnTearDown(string testName, IQuery query) {
			query.Access().DropAllTableConstraints(ObjectName.Parse("APP.test_table"));
			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table2"));
		}

		[Test]
		public void AddColumn() {
			var tableName = ObjectName.Parse("test_table");

			Query.AddColumn(tableName, "reserved", PrimitiveTypes.Boolean());

			var testTable = Query.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.IsNotNull(testTable);
			Assert.AreEqual(6, testTable.TableInfo.ColumnCount);
		}

		[Test]
		public void SetDefaultToColumn() {
			var tableName = ObjectName.Parse("APP.test_table");

			Query.SetDefault(tableName, "active", SqlExpression.Constant(Field.Boolean(false)));

			var testTable = Query.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.IsNotNull(testTable);

			var column = testTable.TableInfo["active"];
			Assert.IsNotNull(column);
			Assert.IsTrue(column.HasDefaultExpression);
			Assert.IsNotNull(column.DefaultExpression);
		}

		[Test]
		public void DropDefaultFromColumn() {
			var tableName = ObjectName.Parse("APP.test_table");

			Query.DropDefault(tableName, "id");

			var testTable = Query.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.IsNotNull(testTable);

			var column = testTable.TableInfo["id"];
			Assert.IsNotNull(column);
			Assert.IsFalse(column.HasDefaultExpression);
			Assert.IsNull(column.DefaultExpression);
		}

		[Test]
		public void AddForeignKeyConstraint() {
			var tableName = ObjectName.Parse("APP.test_table2");
			var constraint = new SqlTableConstraint("FK_1", ConstraintType.ForeignKey, new[] { "person_id" }) {
				ReferenceTable = "APP.test_table",
				ReferenceColumns = new[] { "id" }
			};

			Query.AddConstraint(tableName, constraint);

			var fkeys = Query.Session.Access().QueryTableForeignKeys(tableName);

			Assert.IsNotNull(fkeys);
			Assert.IsNotEmpty(fkeys);

			var fkey = fkeys.FirstOrDefault(x => x.ConstraintName == "FK_1");
			Assert.IsNotNull(fkey);
			Assert.IsNotNull(fkey.ForeignTable);
			Assert.AreEqual("APP.test_table", fkey.ForeignTable.FullName);
			Assert.IsNotNull(fkey.ForeignColumnNames);
			Assert.IsNotEmpty(fkey.ForeignColumnNames);
		}

		[Test]
		public void DropColumn() {
			var tableName = ObjectName.Parse("APP.test_table");

			Query.DropColumn(tableName, "active");

			var testTable = Query.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.IsNotNull(testTable);

			Assert.AreEqual(-1, testTable.TableInfo.IndexOfColumn("active"));
		}

		[Test]
		public void DropConstraint() {
			var tableName = ObjectName.Parse("APP.test_table2");

			Query.DropConstraint(tableName, "FK_1");

			var fkeys = Query.Session.Access().QueryTableForeignKeys(tableName);

			Assert.IsNotNull(fkeys);
			Assert.IsEmpty(fkeys);
		}

		[Test]
		public void DropPrimary() {
			var tableName = ObjectName.Parse("APP.test_table");

			Query.DropPrimaryKey(tableName);

			var pkey = Query.Session.Access().QueryTablePrimaryKey(tableName);

			Assert.IsNull(pkey);
		}
	}
}
