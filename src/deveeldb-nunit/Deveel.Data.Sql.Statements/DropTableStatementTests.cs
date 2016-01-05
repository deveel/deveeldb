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

using Deveel.Data;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class DropTableStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			CreateTestTables(query);
			return query;
		}

		private void CreateTestTables(IQuery context) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var tableInfo1 = new TableInfo(tn1);
			tableInfo1.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo1.AddColumn(new ColumnInfo("name", PrimitiveTypes.String()));
			tableInfo1.AddColumn(new ColumnInfo("date", PrimitiveTypes.DateTime()));
			context.CreateTable(tableInfo1);
			context.AddPrimaryKey(tn1, "id");

			var tn2 = ObjectName.Parse("APP.test_table2");
			var tableInfo2 = new TableInfo(tn2);
			tableInfo2.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo2.AddColumn(new ColumnInfo("other_id", PrimitiveTypes.Integer()));
			tableInfo2.AddColumn(new ColumnInfo("count", PrimitiveTypes.Integer()));
			context.CreateTable(tableInfo2);
			context.AddPrimaryKey(tn2, "id");
			context.AddForeignKey(tn2, new[] { "other_id" }, tn1, new[] { "id" }, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, null);
		}

		[Test]
		public void DropNonReferencedTable() {
			var tableName = ObjectName.Parse("APP.test_table2");
			var statement = new DropTableStatement(tableName);

			statement.Execute(Query);

			var exists = Query.TableExists(tableName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void DropReferencedTable() {
			const string sql = "DROP TABLE APP.test_table1";

			Assert.Throws<StatementException>(() => Query.ExecuteQuery(sql));
		}

		[Test]
		public void DropAllTables() {
			const string sql = "DROP TABLE IF EXISTS APP.test_table1, test_table2";

			Assert.DoesNotThrow(() => Query.ExecuteQuery(sql));
		}
	}
}
