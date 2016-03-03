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
		}

		[Test]
		public void AlterTableAddColumn() {
			var tableName = ObjectName.Parse("APP.test_table");
			var column = new SqlTableColumn("reserved", PrimitiveTypes.Boolean());
			var statement = new AlterTableStatement(tableName, new AddColumnAction(column));

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(Query));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0,  ((SqlNumber) result.GetValue(0,0).Value).ToInt32());

			var testTable = Query.GetTable(new ObjectName("test_table"));

			Assert.IsNotNull(testTable);
			Assert.AreEqual(6, testTable.TableInfo.ColumnCount);
		}
	}
}
