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

using Deveel.Data;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public sealed class SqlQueryExpressionTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTable();
			AddTestData();
		}

		private void CreateTestTable() {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] {SqlExpression.Reference(tableInfo.TableName)});
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			Query.Session.Access().CreateTable(tableInfo, false);
			Query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void AddTestData() {
			var table = Query.Access().GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();
			row.SetValue("first_name", Field.String("John"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", Field.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("first_name", Field.String("Jane"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("first_name", Field.String("Roger"));
			row.SetValue("last_name", Field.String("Rabbit"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);
		}

		[Test]
		[Category("System")]
		public void ExecuteSelectAll() {
			var expression =
				new SqlQueryExpression(new[] {new SelectColumn(SqlExpression.Reference(new ObjectName("first_name")))});
			expression.FromClause.AddTable("test_table");

			Field result = null;
			Assert.DoesNotThrow(() => result = expression.EvaluateToConstant(Query, null));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<QueryType>(result.Type);
			Assert.IsNotNull(result.Value);
			Assert.IsInstanceOf<SqlQueryObject>(result.Value);

			ITable queryResult = null;

			Assert.DoesNotThrow(() => queryResult = ((SqlQueryObject) result.Value).QueryPlan.Evaluate(Query));
			Assert.IsNotNull(queryResult);
			Assert.AreEqual(3, queryResult.RowCount);
		}

		[Test]
		[Category("SQL Parse")]
		public void ParseSelectWithFromClause() {
			const string sql = "SELECT col1 AS a FROM table";

			var expression = SqlExpression.Parse(sql);
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
		[Category("SQL Parse")]
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
		[Category("SQL Parse")]
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
		[Category("SQL Parse")]
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
		[Category("SQL Parse")]
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
		[Category("SQL Parse")]
		public void ParseSimpleQuery() {
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

		[Test]
		[Category("SQL Parse")]
		public void ParseSelectGroupBy() {
			const string sql = "SELECT col1 AS a, AVG(col2) b FROM table WHERE b > 2 GROUP BY a";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(sql));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			var queryExpression = (SqlQueryExpression)expression;
			Assert.IsNotEmpty(queryExpression.SelectColumns);

			var groupBy = queryExpression.GroupBy;
			Assert.IsNotNull(groupBy);
			Assert.IsNotEmpty(groupBy);
			Assert.AreEqual(1, groupBy.Count());
		}
	}
}