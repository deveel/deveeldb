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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data {
	[TestFixture]
	public sealed class InsertSelectTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestTable(query);
			return true;
		}

		private static void CreateTestTable(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			query.Access().CreateTable(table => table
				.Named(tableName)
				.WithColumn(column => column
					.Named("id")
					.HavingType(PrimitiveTypes.Integer())
					.WithDefault(SqlExpression.FunctionCall("UNIQUEKEY",
						new SqlExpression[] {SqlExpression.Constant(tableName.FullName)})))
				.WithColumn("first_name", PrimitiveTypes.String())
				.WithColumn("last_name", PrimitiveTypes.String())
				.WithColumn("birth_date", PrimitiveTypes.DateTime())
				.WithColumn("active", PrimitiveTypes.Boolean()));

			query.Session.Access().AddPrimaryKey(tableName, "id", "PK_TEST_TABLE");
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");

			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void WithColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT 'Antonello', 'Provenzano', NULL, NULL");
			var columns = new[] { "first_name", "last_name", "birth_date", "active" };

			var count = AdminQuery.InsertSelect(tableName, columns, query);

			Assert.AreEqual(1, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);
		}

		[Test]
		public void WithNoColumns() {
			var tableName = ObjectName.Parse("APP.test_table");
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT 3, 'Antonello', 'Provenzano', NULL, NULL");

			var count = AdminQuery.InsertSelect(tableName, query);

			Assert.AreEqual(1, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.AreEqual(1, table.RowCount);

		}
	}
}
