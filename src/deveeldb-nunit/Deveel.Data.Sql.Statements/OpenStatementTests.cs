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

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class OpenStatementTests : ContextBasedTest {
		[Test]
		public void OpenCursorWithoutArguments() {
			const string sql = "OPEN c1";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);
			Assert.IsInstanceOf<OpenStatement>(statementList[0]);

			var statement = (OpenStatement) statementList[0];
			Assert.AreEqual("c1", statement.CursorName);
			Assert.IsEmpty(statement.Arguments);
		}

		[Test]
		public void OpenCursorWithArguments() {
			const string sql = "OPEN c1(34, 'user')";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);
			Assert.IsInstanceOf<OpenStatement>(statementList[0]);

			var statement = (OpenStatement)statementList[0];
			Assert.AreEqual("c1", statement.CursorName);
			Assert.IsNotEmpty(statement.Arguments);

			var arg1 = statement.Arguments.First();
			Assert.IsNotNull(arg1);
			Assert.IsInstanceOf<SqlConstantExpression>(arg1);
		}
	}
}
