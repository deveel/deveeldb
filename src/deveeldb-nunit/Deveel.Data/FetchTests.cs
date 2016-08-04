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
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class FetchTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

			AdminQuery.Access().CreateTable(tableInfo);

			var table = AdminQuery.Access().GetMutableTable(tableName);
			for (int i = 0; i < 10; i++) {
				var row = table.NewRow();
				row.SetValue(0, i);
				row.SetValue(1, String.Format("ID: {0}", i));
				table.AddRow(row);
			}

			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");

			var cursorInfo = new CursorInfo("c1", query);
			cursorInfo.Flags = CursorFlags.Scroll;

			AdminQuery.Access().CreateObject(cursorInfo);
			var c1 = (Cursor)AdminQuery.Access().GetObject(DbObjectType.Cursor, new ObjectName("c1"));
			c1.Open(AdminQuery);

			if (testName.StartsWith("FetchInto")) {
				var query2 = (SqlQueryExpression)SqlExpression.Parse("SELECT a FROM APP.test_table");

				var cursorInfo2 = new CursorInfo("c2", query2);
				cursorInfo2.Flags = CursorFlags.Scroll;

				AdminQuery.Access().CreateObject(cursorInfo2);
				var c2 = (Cursor)AdminQuery.Access().GetObject(DbObjectType.Cursor, new ObjectName("c2"));
				c2.Open(AdminQuery);

				AdminQuery.Access().CreateObject(new VariableInfo("var1", PrimitiveTypes.BigInt(), false));
				AdminQuery.Access().CreateObject(new VariableInfo("var2", PrimitiveTypes.String(), false));
			}
		}

		[Test]
		public void FetchNext() {
			var row = AdminQuery.FetchNext("c1");

			Assert.IsNotNull(row);
			Assert.AreEqual(2, row.ColumnCount);

			var val1 = row.GetValue(0);
			var val2 = row.GetValue(1);

			Assert.IsNotNull(val1);
			Assert.IsNotNull(val2);

			Assert.IsInstanceOf<NumericType>(val1.Type);
			Assert.IsInstanceOf<StringType>(val2.Type);

			var id = ((SqlNumber) val1.Value).ToInt32();
			Assert.AreEqual(0, id);
		}

		[Test]
		public void FetchAbsolute() {
			var row = AdminQuery.FetchAbsolute("c1", SqlExpression.Constant(5));

			Assert.IsNotNull(row);
			Assert.AreEqual(2, row.ColumnCount);

			var val1 = row.GetValue(0);
			var val2 = row.GetValue(1);

			Assert.IsNotNull(val1);
			Assert.IsNotNull(val2);

			Assert.IsInstanceOf<NumericType>(val1.Type);
			Assert.IsInstanceOf<StringType>(val2.Type);

			var id = ((SqlNumber)val1.Value).ToInt32();
			Assert.AreEqual(5, id);
		}

		[Test]
		public void FetchIntoOneVar() {
			AdminQuery.FetchNextInto("c2", SqlExpression.VariableReference("var1"));

			var var1 = (Variable) AdminQuery.Access().GetObject(DbObjectType.Variable, new ObjectName("var1"));

			Assert.IsNotNull(var1);
			Assert.IsNotNull(var1.Evaluate(AdminQuery));
			Assert.IsFalse(Field.IsNullField(var1.Evaluate(AdminQuery)));

			var value = ((SqlNumber) var1.Evaluate(AdminQuery).Value).ToInt32();
			Assert.AreEqual(0, value);
		}

		[Test]
		public void FetchIntoTwoVars() {
			var tuple = SqlExpression.Tuple(SqlExpression.VariableReference("var1"), SqlExpression.VariableReference("var2"));

			AdminQuery.FetchNextInto("c1", tuple);

			var var1 = (Variable)AdminQuery.Access().GetObject(DbObjectType.Variable, new ObjectName("var1"));

			Assert.IsNotNull(var1);
			Assert.IsNotNull(var1.Evaluate(AdminQuery));
			Assert.IsFalse(Field.IsNullField(var1.Evaluate(AdminQuery)));

			var value = ((SqlNumber)var1.Evaluate(AdminQuery).Value).ToInt32();
			Assert.AreEqual(0, value);
		}
	}
}
