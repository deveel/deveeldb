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
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class SelectIntoTests : SqlCompileTestBase {
		[Test]
		public void SelectIntoTable() {
			const string sql = "SELECT a INTO table2 FROM test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectIntoStatement>(statement);

			var selectInto = (SelectIntoStatement) statement;
			Assert.IsNotNull(selectInto);
			Assert.IsNotNull(selectInto.QueryExpression);
			Assert.IsNotNull(selectInto.Reference);
			Assert.IsInstanceOf<SqlReferenceExpression>(selectInto.Reference);
		}

		[Test]
		public void SelectIntoOneVariable() {
			const string sql = "SELECT a INTO :a FROM test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectIntoStatement>(statement);

			var selectInto = (SelectIntoStatement)statement;
			Assert.IsNotNull(selectInto);
			Assert.IsNotNull(selectInto.QueryExpression);
			Assert.IsNotNull(selectInto.Reference);
			Assert.IsInstanceOf<SqlTupleExpression>(selectInto.Reference);
		}

		[Test]
		public void SelectIntoMultipleVariables() {
			const string sql = "SELECT a, b INTO :a, :b FROM test_table";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectIntoStatement>(statement);

			var selectInto = (SelectIntoStatement)statement;
			Assert.IsNotNull(selectInto);
			Assert.IsNotNull(selectInto.QueryExpression);
			Assert.IsNotNull(selectInto.Reference);
			Assert.IsInstanceOf<SqlTupleExpression>(selectInto.Reference);
		}
	}
}
