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
	public sealed class DeclareCursorTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			AdminQuery.Access().CreateTable(table => table
				.Named("APP.test_table")
				.WithColumn("a", PrimitiveTypes.Integer())
				.WithColumn("b", PrimitiveTypes.String()));
		}

		[Test]
		public void InsensitiveSimpleCursor() {
			const string cursorName = "c";
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");

			AdminQuery.DeclareCursor(cursorName, query);

			var cursor = AdminQuery.Context.FindCursor(cursorName);
			Assert.IsNotNull(cursor);
			Assert.AreEqual(cursorName, cursor.CursorInfo.CursorName);
			Assert.IsEmpty(cursor.CursorInfo.Parameters);
		}

		[Test]
		public void InsensitiveWithParams() {
			const string cursorName = "c";
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a = :a");
			var parameters = new[] { new CursorParameter("a", PrimitiveTypes.Integer()) };

			AdminQuery.DeclareCursor(cursorName, parameters, query);

			var cursor = AdminQuery.Context.FindCursor(cursorName);
			Assert.IsNotNull(cursor);
			Assert.AreEqual(cursorName, cursor.CursorInfo.CursorName);
			Assert.IsNotEmpty(cursor.CursorInfo.Parameters);
		}

	}
}
