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
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class UpdateTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestTable(testName, query);
			AddTestData(testName, query);
			return true;
		}

		private static void CreateTestTable(string testName, IQuery context) {
			var tableName1 = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName1);
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] {SqlExpression.Constant(tableInfo.TableName.FullName)});
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			context.Session.Access().CreateTable(tableInfo);
			context.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			if (testName.EndsWith("ConstraintCheck") ||
			    testName.EndsWith("Violation")) {
				tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
				tableInfo.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()) {
					DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
						new SqlExpression[] {SqlExpression.Constant(tableInfo.TableName.FullName)})
				});
				if (testName.StartsWith("SetDefault")) {
					tableInfo.AddColumn(new ColumnInfo("person_id", PrimitiveTypes.Integer()) {
						DefaultExpression = SqlExpression.Constant(0)
					});
				} else if (testName.EndsWith("Violation")) {
					tableInfo.AddColumn("person_id", PrimitiveTypes.Integer(), true);
				} else {
					tableInfo.AddColumn("person_id", PrimitiveTypes.Integer(), false);
				}

				tableInfo.AddColumn("dept_no", PrimitiveTypes.Integer());

				context.Access().CreateTable(tableInfo);
				context.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE2");

				ForeignKeyAction? onUpdate = null;
				if (testName.StartsWith("SetNull")) {
					onUpdate = ForeignKeyAction.SetNull;
				} else if (testName.StartsWith("SetDefault")) {
					onUpdate = ForeignKeyAction.SetDefault;
				} else if (testName.StartsWith("Cascade")) {
					onUpdate = ForeignKeyAction.Cascade;
				}

				if (onUpdate != null)
					context.Access()
						.AddForeignKey(tableInfo.TableName, new[] {"person_id"}, tableName1, new[] {"id"}, ForeignKeyAction.NoAction,
							onUpdate.Value, "FKEY_TEST_TABLE2");
			}
		}

		private static void AddTestData(string testName, IQuery context) {
			var table = context.Access().GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();
			row.SetValue("id", Field.Integer(0));
			row.SetValue("first_name", Field.String("John"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", Field.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("id", Field.Integer(1));
			row.SetValue("first_name", Field.String("Jane"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("id", Field.Integer(2));
			row.SetValue("first_name", Field.String("Roger"));
			row.SetValue("last_name", Field.String("Rabbit"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);

			if (testName.EndsWith("Violation") ||
			    testName.EndsWith("ConstraintCheck")) {
				table = context.Access().GetMutableTable(ObjectName.Parse("APP.test_table2"));
				row = table.NewRow();
				row.SetValue("person_id", Field.Integer(1));
				row.SetValue("dept_no", Field.Integer(45));
				row.SetDefault(context);
				table.AddRow(row);
			}
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
		public void UpdateOneRow() {
			var tableName = ObjectName.Parse("APP.test_table");
			var whereExp = SqlExpression.Parse("id = 2");
			var assignments = new[] {
				new SqlColumnAssignment("birth_date", SqlExpression.Constant(Field.Date(new SqlDateTime(1970, 01, 20))))
			};

			var count = Query.Update(tableName, whereExp, assignments);

			Assert.AreEqual(1, count);
		}

		[Test]
		public void SetNullOnUpdateConstraintCheck() {
			var query = CreateQuery(CreateAdminSession(Database));

			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Jane' AND last_name = 'Doe'");

			var count = query.Update(tableName, expr, new [] {
				new SqlColumnAssignment("id", SqlExpression.Constant(10))  
			});

			query.Commit();

			Assert.AreEqual(1, count);

			query = CreateQuery(CreateAdminSession(Database));

			var linkedTable = query.Access().GetTable(ObjectName.Parse("APP.test_table2"));
			var rows = linkedTable.GetIndex(0).SelectEqual(Field.Integer(1));
			var value = linkedTable.GetValue(rows.First(), 1);

			Assert.IsTrue(Field.IsNullField(value));
		}

		[Test]
		public void CascadeOnUpdateConstraintCheck() {
			var query = CreateQuery(CreateAdminSession(Database));

			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Jane' AND last_name = 'Doe'");

			var count = query.Update(tableName, expr, new[] {
				new SqlColumnAssignment("id", SqlExpression.Constant(10))
			});

			query.Commit();

			Assert.AreEqual(1, count);

			query = CreateQuery(CreateAdminSession(Database));

			var linkedTable = query.Access().GetTable(ObjectName.Parse("APP.test_table2"));
			var rows = linkedTable.GetIndex(0).SelectEqual(Field.Integer(1));
			var value = linkedTable.GetValue(rows.First(), 1);

			Assert.IsFalse(Field.IsNullField(value));
			Assert.AreEqual(10, ((SqlNumber)value.Value).ToInt32());
		}

		[Test]
		public void SetDefaultOnUpdateConstraintCheck() {
			var query = CreateQuery(CreateAdminSession(Database));

			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Jane' AND last_name = 'Doe'");

			var count = query.Update(tableName, expr, new[] {
				new SqlColumnAssignment("id", SqlExpression.Constant(10))
			});

			query.Commit();

			Assert.AreEqual(1, count);

			query = CreateQuery(CreateAdminSession(Database));

			var linkedTable = query.Access().GetTable(ObjectName.Parse("APP.test_table2"));
			var rows = linkedTable.GetIndex(0).SelectEqual(Field.Integer(1));
			var value = linkedTable.GetValue(rows.First(), 1);

			Assert.IsFalse(Field.IsNullField(value));
			Assert.AreEqual(0, ((SqlNumber)value.Value).ToInt32());
		}
	}
}
