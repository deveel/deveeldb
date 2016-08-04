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
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OpenCursorTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);
			AdminQuery.Access().CreateTable(tableInfo);
			DeclareCursors(AdminQuery);
		}

		private static void DeclareCursors(IQuery query) {
			var queryExp1 = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM APP.test_table");
			query.Context.DeclareCursor(query, "c1", queryExp1);


			var queryExp2 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a > :a");
			var cursorInfo = new CursorInfo("c2", queryExp2);
			cursorInfo.Parameters.Add(new CursorParameter("a", PrimitiveTypes.Integer()));
			query.Context.DeclareCursor(cursorInfo,query);
		}

		[Test]
		public void Simple() {
			AdminQuery.Open("c1");

			var cursor = AdminQuery.Context.FindCursor("c1");
			Assert.IsNotNull(cursor);
			Assert.AreEqual("c1", cursor.CursorInfo.CursorName);
			Assert.AreEqual(CursorStatus.Open, cursor.Status);
			Assert.IsNotNull(cursor.Source);
		}

		protected override void AssertNoErrors(string testName) {
			if (!testName.EndsWith("Violation"))
				base.AssertNoErrors(testName);
		}

		[Test]
		public void DoubleOpenViolation() {
			AdminQuery.Open("c1");

			var expected = Is.InstanceOf<SqlErrorException>()
				.And.TypeOf<CursorOpenException>();

			Assert.Throws(expected, () => AdminQuery.Open("c1"));
		}
	}
}
