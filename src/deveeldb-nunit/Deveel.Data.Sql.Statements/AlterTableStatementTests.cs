// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class AlterTableStatementTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTable();
		}

		private void CreateTestTable() {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			Query.CreateTable(tableInfo);
			Query.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			Query.CreateTable(tableInfo);

			if (TestContext.CurrentContext.Test.Name == "DropConstraint") {
				Query.AddForeignKey(tableInfo.TableName, new string[] {"person_id"}, ObjectName.Parse("APP.test_table"),
					new[] {"id"}, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, "FK_1");
			}
		}

		[Test]
		public void AddColumn() {
			var tableName = ObjectName.Parse("APP.test_table");
			var column = new SqlTableColumn("reserved", PrimitiveTypes.Boolean());
			var statement = new AlterTableStatement(tableName, new AddColumnAction(column));

			ITable result = null;
			Assert.DoesNotThrow(() => result = Query.ExecuteStatement(statement));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0,  ((SqlNumber) result.GetValue(0,0).Value).ToInt32());

			var testTable = Query.GetTable(new ObjectName("test_table"));

			Assert.IsNotNull(testTable);
			Assert.AreEqual(6, testTable.TableInfo.ColumnCount);
		}

		[Test]
		public void SetDefaultToColumn() {
			var tableName = ObjectName.Parse("APP.test_table");
			var action = new SetDefaultAction("active", SqlExpression.Constant(Field.Boolean(false)));
			var statement = new AlterTableStatement(tableName,  action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var testTable = Query.GetTable(new ObjectName("test_table"));

			Assert.IsNotNull(testTable);

			var column = testTable.TableInfo["active"];
			Assert.IsNotNull(column);
			Assert.IsTrue(column.HasDefaultExpression);
			Assert.IsNotNull(column.DefaultExpression);
		}

		[Test]
		public void DropDefaultFromColumn() {
			var tableName = ObjectName.Parse("APP.test_table");
			var action = new DropDefaultAction("id");
			var statement = new AlterTableStatement(tableName, action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var testTable = Query.GetTable(new ObjectName("test_table"));

			Assert.IsNotNull(testTable);

			var column = testTable.TableInfo["id"];
			Assert.IsNotNull(column);
			Assert.IsFalse(column.HasDefaultExpression);
			Assert.IsNull(column.DefaultExpression);
		}

		[Test]
		public void AddForeignKeyConstraint() {
			var tableName = ObjectName.Parse("APP.test_table2");
			var constraint = new SqlTableConstraint("FK_1", ConstraintType.ForeignKey, new[] {"person_id"}) {
				ReferenceTable = "APP.test_table",
				ReferenceColumns = new[] {"id"}
			};

			var action = new AddConstraintAction(constraint);
			var statement = new AlterTableStatement(tableName, action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var fkeys = Query.GetTableForeignKeys(tableName);

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
			var action = new DropColumnAction("active");
			var statement = new AlterTableStatement(tableName, action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var testTable = Query.GetTable(new ObjectName("test_table"));

			Assert.IsNotNull(testTable);

			Assert.AreEqual(-1, testTable.TableInfo.IndexOfColumn("active"));
		}

		[Test]
		public void DropConstraint() {
			var tableName = ObjectName.Parse("APP.test_table2");
			var action = new DropConstraintAction("FK_1");
			var statement = new AlterTableStatement(tableName, action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var fkeys = Query.GetTableForeignKeys(tableName);

			Assert.IsNotNull(fkeys);
			Assert.IsEmpty(fkeys);
		}

		[Test]
		public void DropPrimary() {
			var tableName = ObjectName.Parse("APP.test_table");
			var action = new DropPrimaryKeyAction();
			var statement = new AlterTableStatement(tableName, action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			var pkey = Query.GetTablePrimaryKey(tableName);

			Assert.IsNull(pkey);
		}
	}
}
