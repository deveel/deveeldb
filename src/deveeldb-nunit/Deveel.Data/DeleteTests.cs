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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeleteTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestTable(testName, query);
			InsertTestData(testName, query);
			return true;
		}

		private void CreateTestTable(string testName, IQuery query) {
			var tableName1 = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName1);
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			if (testName.EndsWith("WithLob")) {
				tableInfo.AddColumn("bio", PrimitiveTypes.Clob());
			}

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			if (testName.EndsWith("ConstraintCheck") ||
				testName.EndsWith("Violation")) {
				tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
				tableInfo.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()) {
					DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
						new SqlExpression[] {SqlExpression.Constant(tableInfo.TableName.FullName)})
				});
				if (testName.EndsWith("Violation")) {
					tableInfo.AddColumn("person_id", PrimitiveTypes.Integer(), true);
				} else {
					tableInfo.AddColumn("person_id", PrimitiveTypes.Integer(), false);
				}

				tableInfo.AddColumn("dept_no", PrimitiveTypes.Integer());

				query.Access().CreateTable(tableInfo);
				query.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE2");

				if (testName.StartsWith("SetNullOnDelete")) {
					query.Access()
						.AddForeignKey(tableInfo.TableName, new[] {"person_id"}, tableName1, new[] {"id"}, ForeignKeyAction.SetNull,
							ForeignKeyAction.NoAction, "FKEY_TEST_TABLE2");
				} else if (testName.StartsWith("CascadeOnDelete")) {
					query.Access()
						.AddForeignKey(tableInfo.TableName, new[] {"person_id"}, tableName1, new[] {"id"}, ForeignKeyAction.Cascade,
							ForeignKeyAction.NoAction, "FKEY_TEST_TABLE2");
				}
			}
		}

		private void InsertTestData(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			var table = query.Access().GetMutableTable(tableName);
			var row = table.NewRow();
			row.SetValue("first_name", Field.String("Antonello"));
			row.SetValue("last_name", Field.String("Provenzano"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1980, 06, 04)));
			row.SetValue("active", Field.BooleanTrue);
			if (testName.EndsWith("WithLob"))
				row.SetValue("bio", Field.Clob(CreateClobData(query)));

			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("first_name", Field.String("Sebastiano"));
			row.SetValue("last_name", Field.String("Provenzano"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1981, 08, 26)));
			row.SetValue("active", Field.BooleanFalse);
			row.SetDefault(query);
			table.AddRow(row);

			if (testName.EndsWith("Violation") ||
			    testName.EndsWith("ConstraintCheck")) {
				table = query.Access().GetMutableTable(ObjectName.Parse("APP.test_table2"));
				row = table.NewRow();
				row.SetValue("person_id", Field.Integer(1));
				row.SetValue("dept_no", Field.Integer(45));
				row.SetDefault(query);
				table.AddRow(row);
			}
		}

		private SqlLongString CreateClobData(IQuery query) {
			const string text = "One simple small string to trigger the LOB data for the test";
			return SqlLongString.Ascii(query, text);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			if (testName.EndsWith("Violation") ||
			    testName.EndsWith("ConstraintCheck")) {
				tableName = ObjectName.Parse("APP.test_table2");
				query.Access().DropAllTableConstraints(tableName);
				query.Access().DropObject(DbObjectType.Table, tableName);
			}

			return true;
		}

		[Test]
		public void OnlyOneRow() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Antonello'");

			var count = Query.Delete(tableName, expr);
			Assert.AreEqual(1, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void TwoRows() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var count = Query.Delete(tableName, expr);
			Assert.AreEqual(2, count);

			var table = Query.Access().GetTable(tableName);

			Assert.AreEqual(0, table.RowCount);
		}

		[Test]
		public void WithLob() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var query = CreateQuery(CreateAdminSession(Database));

			var count = query.Delete(tableName, expr);
			query.Commit();

			Assert.AreEqual(2, count);

			query = CreateQuery(CreateAdminSession(Database));
			var table = query.Access().GetTable(tableName);

			Assert.AreEqual(0, table.RowCount);
		}

		[Test]
		public void SetNullOnDeleteConstraintCheck() {
			var query = CreateQuery(CreateAdminSession(Database));

			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var count = query.Delete(tableName, expr);
			query.Commit();

			Assert.AreEqual(2, count);

			query = CreateQuery(CreateAdminSession(Database));

			var linkedTable = query.Access().GetTable(ObjectName.Parse("APP.test_table2"));
			var rows = linkedTable.GetIndex(0).SelectEqual(Field.Integer(1));
			var value = linkedTable.GetValue(rows.First(), 1);

			Assert.IsTrue(Field.IsNullField(value));
		}
	}
}
