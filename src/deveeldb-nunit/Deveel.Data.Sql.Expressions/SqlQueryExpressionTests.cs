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

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public sealed class SqlQueryExpressionTests {
		[Test]
		public void ParseSelectWithFromClause() {
			const string sql = "SELECT col1 AS a FROM table";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression) expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.First().Expression);
			Assert.AreEqual("a", queryExpression.SelectColumns.First().Alias);
			Assert.IsNotNull(queryExpression.FromClause);
			Assert.AreEqual(1, queryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("table", queryExpression.FromClause.AllTables.First().Name);
		}

		[Test]
		public void ParseSelectWithNaturalJoin() {
			const string sql = "SELECT a.col1, b.col2 FROM table1 a, table2 b";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression) expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.First().Expression);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.Skip(1).First().Expression);
			Assert.IsNotNull(queryExpression.FromClause);
			Assert.IsNotEmpty(queryExpression.FromClause.AllTables);
			Assert.AreEqual(2, queryExpression.FromClause.AllTables.Count());
			Assert.AreEqual(1, queryExpression.FromClause.JoinPartCount);
			Assert.IsNotNull(queryExpression.FromClause.GetJoinPart(0));
			Assert.AreEqual(JoinType.Inner, queryExpression.FromClause.GetJoinPart(0).JoinType);
		}

		[Test]
		public void ParseSelectWithInnerJoin() {
			const string sql = "SELECT a.col1, b.col2 FROM table1 AS a INNER JOIN table2 b ON a.id = b.id";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression) expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.First().Expression);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.Skip(1).First().Expression);
			Assert.IsNotNull(queryExpression.FromClause);
			Assert.IsNotEmpty(queryExpression.FromClause.AllTables);
			Assert.AreEqual(2, queryExpression.FromClause.AllTables.Count());
			Assert.AreEqual(1, queryExpression.FromClause.JoinPartCount);
			Assert.IsNotNull(queryExpression.FromClause.GetJoinPart(0));
			Assert.AreEqual(JoinType.Inner, queryExpression.FromClause.GetJoinPart(0).JoinType);
			Assert.IsNotNull(queryExpression.FromClause.GetJoinPart(0).OnExpression);
			Assert.IsInstanceOf<SqlBinaryExpression>(queryExpression.FromClause.GetJoinPart(0).OnExpression);
		}

		[Test]
		public void ParseSelectFunction() {
			const string sql = "SELECT user()";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression) expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(queryExpression.SelectColumns.First().Expression);
			Assert.AreEqual("user", ((SqlFunctionCallExpression) queryExpression.SelectColumns.First().Expression).FunctioName.FullName);
		}

		[Test]
		public void ParseSelectSubQuery() {
			const string sql = "SELECT * FROM (SELECT a, b FROM table1)";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression) expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsNotEmpty(queryExpression.FromClause.AllTables);
			Assert.AreEqual(1, queryExpression.FromClause.AllTables.Count());
			Assert.IsTrue(queryExpression.FromClause.AllTables.First().IsSubQuery);
		}

		//[Test]
		//public void FluidSelectWithClause() {
		//	var expression = SqlQueryBuilder.Configure().Items(list => list.Column("col1", "a")).From("table").AsExpression();
		//}

		[Test]
		public void ExecuteSimpleQuery() {
			const string sql = "SELECT col1 AS a FROM table";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression)expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);
			Assert.IsInstanceOf<SqlReferenceExpression>(queryExpression.SelectColumns.First().Expression);
			Assert.AreEqual("a", queryExpression.SelectColumns.First().Alias);
			Assert.IsNotNull(queryExpression.FromClause);
			Assert.AreEqual(1, queryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("table", queryExpression.FromClause.AllTables.First().Name);
		}
	}
}