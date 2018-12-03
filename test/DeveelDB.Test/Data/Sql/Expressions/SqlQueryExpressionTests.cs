// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Linq;

using Deveel.Data.Query;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlQueryExpressionTests {
		[Theory]
		[InlineData("a.*", null, "a.*")]
		[InlineData("a", "b", "a AS b")]
		public static void CreateNewTableSource(string tableName, string alias, string expected) {
			var name = ObjectName.Parse(tableName);
			var source = new SqlQueryExpressionSource(name, alias);

			Assert.True(source.IsTable);
			Assert.False(source.IsQuery);
			Assert.Equal(!String.IsNullOrWhiteSpace(alias), source.IsAliased);
			Assert.Equal(expected, source.ToString());
		}

		[Fact]
		public static void CreateNewQuerySource() {
			var fromTable = ObjectName.Parse("table1");
			var query = new SqlQueryExpression();
			query.AllItems = true;
			query.From.Table(fromTable);
			var source = new SqlQueryExpressionSource(query, "a");

			Assert.True(source.IsQuery);
			Assert.False(source.IsTable);
			Assert.True(source.IsAliased);
			Assert.Equal("a", source.Alias);
			Assert.True(source.Query.AllItems);

			var expected = new SqlStringBuilder();
			expected.AppendLine("(SELECT *");
			expected.Append("  FROM table1) AS a");

			Assert.Equal(expected.ToString(), source.ToString());
		}

		[Fact]
		public static void CreateNewSimpleQuery() {
			var query = new SqlQueryExpression();
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("a.*")));
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("b")));
			query.From.Table(ObjectName.Parse("tab1"));


			var expected = new SqlStringBuilder();
			expected.AppendLine("SELECT a.*, b");
			expected.Append("  FROM tab1");

			Assert.False(query.From.IsEmpty);
			Assert.Equal(expected.ToString(), query.ToString());
		}

		[Fact]
		public static void MakeNewNaturalJoinQuery() {
			var query = new SqlQueryExpression();
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("a.*")));
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("b.*")));
			query.From.Table(ObjectName.Parse("table1"), "a");
			query.From.Table(ObjectName.Parse("table2"), "b");

			var expected = new SqlStringBuilder();
			expected.AppendLine("SELECT a.*, b.*");
			expected.Append("  FROM table1 AS a, table2 AS b");
			Assert.Equal(expected.ToString(), query.ToString());
		}

		[Fact]
		public static void MakeNewInnerJoinQuery() {
			var query = new SqlQueryExpression();
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("a.*")));
			query.Items.Add(SqlExpression.Reference(ObjectName.Parse("b.*")));
			query.From.Table(ObjectName.Parse("table1"), "a");
			query.From.Join(JoinType.Inner,
				SqlExpression.Equal(SqlExpression.Reference(ObjectName.Parse("a.id")),
					SqlExpression.Reference(ObjectName.Parse("b.a_id"))));
			query.From.Table(ObjectName.Parse("table2"), "b");

			var expected = new SqlStringBuilder();
			expected.AppendLine("SELECT a.*, b.*");
			expected.Append("  FROM table1 AS a INNER JOIN table2 AS b ON a.id = b.a_id");

			Assert.Equal(expected.ToString(), query.ToString());
		}

		[Fact]
		public static void ParseSimpleQuery() {
			const string sql = "SELECT * FROM app.a a_table WHERE a.id > 4";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
		}

		[Fact]
		public static void ParseJoinedQuery() {
			const string sql = "SELECT a.id, b.* FROM a INNER JOIN b ON a.id = b.a_id WHERE b.level >= 3";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(2, query.Items.Count);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Equal(2, query.From.Sources.Count());
		}

		[Fact]
		public static void ParseSubquery() {
			const string sql = "SELECT * FROM (SELECT a FROM b) q WHERE q.a > 5";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
			Assert.IsType<SqlQueryExpressionSource>(query.From.Sources.First());
		}

		[Fact]
		public static void ParseGroupByQuery() {
			const string sql = "SELECT COUNT(*) FROM a GROUP BY a.b";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.Null(query.Where);
			Assert.NotNull(query.GroupBy);
			Assert.NotEmpty(query.GroupBy);
			Assert.Equal(1, query.GroupBy.Count);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
		}

		[Fact]
		public static void ParseGroupMaxQuery() {
			const string sql = "SELECT COUNT(*) FROM a GROUP MAX a.b";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.Null(query.Where);
			Assert.NotNull(query.GroupMax);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
		}

		[Fact]
		public static void ParseSelectInArray() {
			const string sql = "SELECT * FROM a WHERE a.b IN (45, 893, 001)";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public static void ParseSelectInQuery() {
			const string sql = "SELECT * FROM a WHERE a.b IN (SELECT a FROM b)";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public static void ParseSelectInVariable() {
			const string sql = "SELECT * FROM a WHERE a.b IN :b";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public static void ParseSelectInto() {
			const string sql = "SELECT * INTO :a1 FROM a WHERE a.id = 22";

			var exp = SqlExpression.Parse(sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}
	}
}