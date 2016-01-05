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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class SelectStatementTests : ContextBasedTest {
		protected override ISession CreateAdminSession(IDatabase database) {
			using (var session = base.CreateAdminSession(database)) {
				using (var query = session.CreateQuery()) {
					CreateTestTable(query);
					AddTestData(query);

					query.Commit();
				}
			}

			return base.CreateAdminSession(database);
		}

		private void CreateTestTable(IQuery context) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			context.CreateTable(tableInfo);
			context.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void AddTestData(IQuery context) {
			var table = context.GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(0));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("John"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", DataObject.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(1));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("Jane"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(2));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("Roger"));
			row.SetValue("last_name", DataObject.String("Rabbit"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);

			context.Commit();
		}

		[Test]
		public void SelectAllColumns() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table");
			var statement = new SelectStatement(query);

			ITable result = statement.Execute(Query);
			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}

		[Test]
		public void SimpleOrderedSelect() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table");
			var sort = new[] {new SortColumn(SqlExpression.Reference(new ObjectName("birth_date")), false)};
			var statement = new SelectStatement(query, sort);

			var result = statement.Execute(Query);
			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);

			var firstName = result.GetValue(0, 1);

			Assert.AreEqual("Roger", firstName.Value.ToString());
		}

		[Test]
		public void SelectAliasedWithGroupedExpression() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM test_table t0 WHERE (t0.id = 1 AND t0.id <> 0)");
			var statement = new SelectStatement(query);

			var result = statement.Execute(Query);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void SelectFromAliased() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM test_table t0 WHERE t0.id = 1");

			var statement = new SelectStatement(query);

			var result = statement.Execute(Query);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}
	}
}
