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

			query.Access().CreateTable(table => {
				table
					.Named(tableName1)
					.WithColumn(column => column
						.Named("id")
						.HavingType(PrimitiveTypes.Integer())
						.WithDefault(SqlExpression.FunctionCall("UNIQUEKEY",
							new SqlExpression[] {SqlExpression.Constant(tableName1.FullName)})))
					.WithColumn("first_name", PrimitiveTypes.String())
					.WithColumn("last_name", PrimitiveTypes.String())
					.WithColumn("birth_date", PrimitiveTypes.DateTime())
					.WithColumn("active", PrimitiveTypes.Boolean());
				if (testName.EndsWith("WithLob"))
					table.WithColumn("bio", PrimitiveTypes.Clob());

			});

			query.Access().AddPrimaryKey(tableName1, "id", "PK_TEST_TABLE");

			if (testName.EndsWith("ConstraintCheck") ||
			    testName.EndsWith("Violation")) {
				var tableName2 = ObjectName.Parse("APP.test_table2");
				query.Access().CreateTable(table => {
					table
						.Named(tableName2)
						.WithColumn(column => column
							.Named("id")
							.HavingType(PrimitiveTypes.Integer())
							.WithDefault(SqlExpression.FunctionCall("UNIQUEKEY",
								new SqlExpression[] {SqlExpression.Constant(tableName2.FullName)})));
					if (testName.StartsWith("SetDefault")) {
						table.WithColumn(column => column
							.Named("person_id")
							.HavingType(PrimitiveTypes.Integer())
							.WithDefault(SqlExpression.Constant(1)));
					} else {
						table.WithColumn(column => column
							.Named("person_id")
							.HavingType(PrimitiveTypes.Integer())
							.NotNull(testName.EndsWith("Violation")));
					}

					table.WithColumn("dept_no", PrimitiveTypes.Integer());
				});

				query.Access().AddPrimaryKey(tableName2, "id", "PK_TEST_TABLE2");

				ForeignKeyAction? onDelete = null;
				if (testName.StartsWith("SetNullOnDelete")) {
					onDelete = ForeignKeyAction.SetNull;
				} else if (testName.StartsWith("SetDefaultOnDelete")) {
					onDelete = ForeignKeyAction.SetDefault;
				} else if (testName.StartsWith("CascadeOnDelete")) {
					onDelete = ForeignKeyAction.Cascade;
				}

				if (onDelete != null)
					query.Access()
						.AddForeignKey(tableName2, new[] {"person_id"}, tableName1, new[] {"id"}, onDelete.Value,
							ForeignKeyAction.NoAction, "FKEY_TEST_TABLE2");

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
				row.SetValue("person_id", Field.Integer(2));
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

			var count = AdminQuery.Delete(tableName, expr);
			Assert.AreEqual(1, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void TwoRows() {
			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("last_name = 'Provenzano'");

			var count = AdminQuery.Delete(tableName, expr);
			Assert.AreEqual(2, count);

			var table = AdminQuery.Access().GetTable(tableName);

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

		[Test]
		public void SetDefaultOnDeleteConstraintCheck() {
			var query = CreateQuery(CreateAdminSession(Database));

			var tableName = ObjectName.Parse("APP.test_table");
			var expr = SqlExpression.Parse("first_name = 'Sebastiano' AND last_name = 'Provenzano'");

			var count = query.Delete(tableName, expr);
			query.Commit();

			Assert.AreEqual(1, count);

			query = CreateQuery(CreateAdminSession(Database));

			var linkedTable = query.Access().GetTable(ObjectName.Parse("APP.test_table2"));
			var rows = linkedTable.GetIndex(0).SelectEqual(Field.Integer(1));
			var value = linkedTable.GetValue(rows.First(), 1);

			Assert.AreEqual(1, ((SqlNumber)value.Value).ToInt32());
		}
	}
}
