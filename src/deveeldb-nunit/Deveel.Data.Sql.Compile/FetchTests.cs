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

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class FetchTests : SqlCompileTestBase {
		[TestCase("NEXT")]
		[TestCase("PRIOR")]
		[TestCase("FIRST")]
		[TestCase("LAST")]
		public void FetchSimpleFromCursorInto(string direction) {
			var sql = String.Format("FETCH {0} FROM test_cursor INTO test_table", direction);

			var expectedDirection = (FetchDirection) Enum.Parse(typeof(FetchDirection), direction, true);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<FetchIntoStatement>(statement);

			var cursorStatement = (FetchIntoStatement) statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.AreEqual(expectedDirection, cursorStatement.Direction);
		}

		[TestCase("ABSOLUTE", 200)]
		[TestCase("RELATIVE", 123)]
		public void FetchPositionalFromCursorInto(string direction, int offset) {
			var sql = String.Format("FETCH {0} {1} FROM test_cursor INTO test_table", direction, offset);

			var expectedDirection = (FetchDirection)Enum.Parse(typeof(FetchDirection), direction, true);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<FetchIntoStatement>(statement);

			var cursorStatement = (FetchIntoStatement)statement;
			Assert.AreEqual("test_cursor", cursorStatement.CursorName);
			Assert.AreEqual(expectedDirection, cursorStatement.Direction);
			Assert.AreEqual(SqlExpression.Constant(offset), cursorStatement.OffsetExpression);
		}


		[TestCase("NEXT")]
		[TestCase("PRIOR")]
		[TestCase("FIRST")]
		[TestCase("LAST")]
		public void FetchImplicitCursor(string direction) {
			var sql = String.Format("FETCH {0}", direction);
			var expectedDirection = (FetchDirection)Enum.Parse(typeof(FetchDirection), direction, true);

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.IsNotEmpty(result.Statements);
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<FetchStatement>(statement);

			var cursorStatement = (FetchStatement) statement;
			Assert.IsNullOrEmpty(cursorStatement.CursorName);
			Assert.AreEqual(expectedDirection, cursorStatement.Direction);
		}
	}
}