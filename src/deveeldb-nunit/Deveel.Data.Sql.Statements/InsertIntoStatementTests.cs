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
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class InsertIntoStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTestTable(query);
			return query;
		}

		private void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.CreateTable(tableInfo);
			query.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void InsertTwoValues() {
			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new [] { "first_name", "last_name", "active"};
			var values = new List<SqlExpression[]> {
				new[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true)
				},
				new [] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false)
				}
			};

			var statement = new InsertStatement(tableName, columns, values);
			Query.ExecuteStatement(statement);

			var table = Query.GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.RowCount);
		}
	}
}
