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

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class PlSqlCodeBlockTests : SqlCompileTestBase {
		[Test]
		public void SelectInBlock() {
			const string sql = @"BEGIN
							SELECT * FROM test WHERE a = 90 AND
													 b > 12.922;
						END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var obj = result.Statements.ElementAt(0);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<PlSqlBlockStatement>(obj);

			var block = (PlSqlBlockStatement) obj;

			Assert.AreEqual(1, block.Statements.Count);
			Assert.AreEqual(0, block.ExceptionHandlers.Count());
			Assert.IsNull(block.Label);

			var statement = block.Statements.ElementAt(0);
			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);
		}

		[Test]
		public void DeclarationsBeforeBlock() {
			const string sql = @"DECLARE a INT := 23
								BEGIN
									SELECT * FROM test WHERE a < test.a;
								END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<PlSqlBlockStatement>(statement);
		}

		[Test]
		public void WithExceptionHandling() {
			const string sql = @"DECLARE a INT := 23
								BEGIN
									SELECT * FROM test WHERE a < test.a;
                                    EXCEPTION WHEN TOO_MANY_ROWS THEN
                                       ROLLBACK;
                                       RAISE
								END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<PlSqlBlockStatement>(statement);

			var block = (PlSqlBlockStatement) statement;
			Assert.IsNotNull(block.ExceptionHandlers);
			Assert.IsNotEmpty(block.ExceptionHandlers);
			Assert.AreEqual(1, block.ExceptionHandlers.Count);

			var handler = block.ExceptionHandlers.ElementAt(0);

			Assert.AreEqual(1, handler.Handled.ExceptionNames.Count());
			Assert.AreEqual("TOO_MANY_ROWS", handler.Handled.ExceptionNames.ElementAt(0));
		}

		[Test]
		public void WithMultipleExceptionsHandling() {
			const string sql = @"DECLARE a INT := 23
								BEGIN
									SELECT * FROM test WHERE a < test.a;
                                    EXCEPTION WHEN TOO_MANY_ROWS THEN
                                       ROLLBACK;
                                       RAISE
                                    EXCEPTION WHEN MY_ERROR THEN
                                       RETURN 0;
								END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<PlSqlBlockStatement>(statement);

			var block = (PlSqlBlockStatement)statement;
			Assert.IsNotNull(block.ExceptionHandlers);
			Assert.IsNotEmpty(block.ExceptionHandlers);
			Assert.AreEqual(2, block.ExceptionHandlers.Count);

			var handler = block.ExceptionHandlers.ElementAt(0);

			Assert.AreEqual(1, handler.Handled.ExceptionNames.Count());
			Assert.AreEqual("TOO_MANY_ROWS", handler.Handled.ExceptionNames.ElementAt(0));
		}

		[Test]
		public void WithNullStatement() {
			const string sql = @"DECLARE a INT := 23
								BEGIN
									:t := :a;
                                    NULL;
								END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<PlSqlBlockStatement>(statement);

			var block = (PlSqlBlockStatement)statement;
			Assert.AreEqual(2, block.Statements.Count);

			var nullStatement = block.Statements.ElementAt(1);

			Assert.IsNotNull(nullStatement);
			Assert.IsInstanceOf<NullStatement>(nullStatement);
		}
	}
}
