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

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class SelectTests : SqlCompileTestBase {
		[Test]
		public void WithFromClause() {
			const string sql = "SELECT col1 AS a FROM table1";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);
		}

		[Test]
		public void WithVariable() {
			const string sql = "SELECT :a";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);

			Assert.IsNotNull(selectStatement.QueryExpression.SelectColumns);

			var selectCols = selectStatement.QueryExpression.SelectColumns.ToList();
			Assert.AreEqual(1, selectCols.Count);
			Assert.IsInstanceOf<SqlVariableReferenceExpression>(selectCols[0].Expression);
		}

		[Test]
		public void WithFunction() {
			const string sql = "SELECT user()";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);

			Assert.IsNotNull(selectStatement.QueryExpression.SelectColumns);

			var selectCols = selectStatement.QueryExpression.SelectColumns.ToList();
			Assert.AreEqual(1, selectCols.Count);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(selectCols[0].Expression);
		}

		[Test]
		public void WithOrderByClause() {
			const string sql = "SELECT col1 AS a FROM table1 ORDER BY a ASC";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNotNull(selectStatement.OrderBy);
		}

		[Test]
		public void CountAll() {
			const string sql = "SELECT COUNT(*) FROM table1";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;

			Assert.IsNotNull(queryExpression.SelectColumns);
			Assert.IsNotEmpty(queryExpression.SelectColumns);

			var item = queryExpression.SelectColumns.First();
			
			Assert.IsNotNull(item);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(item.Expression);

			var funcCall = (SqlFunctionCallExpression) item.Expression;
			Assert.IsNotEmpty(funcCall.Arguments);
			Assert.AreEqual(1, funcCall.Arguments.Length);
			Assert.IsInstanceOf<InvokeArgument>(funcCall.Arguments[0]);
		}

		[Test]
		public void FromGlobbedTable() {
			const string sql = "SELECT table1.* FROM table1";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
			
			Assert.IsNotNull(queryExpression.SelectColumns);
			Assert.IsNotEmpty(queryExpression.SelectColumns);

			var item = queryExpression.SelectColumns.First();

			Assert.IsNotNull(item);
			Assert.IsNotNull(item.ReferenceName);
			Assert.IsTrue(item.IsGlob);
			Assert.AreEqual("table1", item.TableName.FullName);
		}

		[Test]
		public void SelectLimit() {
			const string sql = "SELECT * FROM table1 LIMIT 1,2";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
		}

		[Test]
		public void SelectFunctionOnly() {
			const string sql = "SELECT func()";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
		}

		[Test]
		public void SelectFunctionInTable() {
			const string sql = "SELECT a, func() FROM table1";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
		}

		[Test]
		public void NaturalJoin() {
			const string sql = "SELECT a.*, b.two FROM table1 AS a, table2 b WHERE a.id = b.a_id";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
		}

		[Test]
		public void SelectConcatOperator() {
			const string sql = "SELECT a || b FROM table1 WHERE id > 2";

			var result = Compile(sql);
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);

			var queryExpression = selectStatement.QueryExpression;
		}
	}
}
