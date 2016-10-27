using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Expressions.Build;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements.Build {
	[TestFixture]
	public static class SelectTests {
		[Test]
		public static void SimpleSelect() {
			var statement = SqlStatementBuilder.Select(select => select
				.Query(query => query.AllColumns()
					.FromTable("app.test")));

			Assert.IsNotNull(statement);
			Assert.IsNotNull(statement.QueryExpression);

			Assert.IsNotEmpty(statement.QueryExpression.SelectColumns);
			Assert.AreEqual(1, statement.QueryExpression.SelectColumns.Count());
			Assert.IsTrue(statement.QueryExpression.SelectColumns.ElementAt(0).IsAll);
			Assert.IsNotEmpty(statement.QueryExpression.FromClause.AllTables);
			Assert.AreEqual(1, statement.QueryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("app.test", statement.QueryExpression.FromClause.AllTables.ElementAt(0).Name);
		}

		[Test]
		public static void DistinctSelect() {
			var statement = SqlStatementBuilder.Select(select => select
				.Query(query => query
					.Distinct()
					.AllColumns()
					.FromTable("app.test")));

			Assert.IsNotNull(statement);
			Assert.IsNotNull(statement.QueryExpression);

			Assert.IsTrue(statement.QueryExpression.Distinct);
			Assert.IsNotEmpty(statement.QueryExpression.SelectColumns);
			Assert.AreEqual(1, statement.QueryExpression.SelectColumns.Count());
			Assert.IsTrue(statement.QueryExpression.SelectColumns.ElementAt(0).IsAll);
			Assert.IsNotEmpty(statement.QueryExpression.FromClause.AllTables);
			Assert.AreEqual(1, statement.QueryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("app.test", statement.QueryExpression.FromClause.AllTables.ElementAt(0).Name);
		}

		[Test]
		public static void GroupByColumn() {
			var statement = SqlStatementBuilder.Select(select => select
				.Query(query => query
					.Column("name")
					.Function("COUNT", new SqlExpression[] {SqlExpression.Reference(new ObjectName("*"))})
					.FromTable("app.test")
					.GroupBy(groupBy => groupBy.Reference("name"))
					.Having(having => having.Reference("name").IsNot(not => not.Value(null)))));

			Assert.IsNotNull(statement);
			Assert.IsNotNull(statement.QueryExpression);

			Assert.IsNotEmpty(statement.QueryExpression.SelectColumns);
			Assert.AreEqual(2, statement.QueryExpression.SelectColumns.Count());
			Assert.IsNotEmpty(statement.QueryExpression.FromClause.AllTables);
			Assert.AreEqual(1, statement.QueryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("app.test", statement.QueryExpression.FromClause.AllTables.ElementAt(0).Name);

			Assert.IsNotEmpty(statement.QueryExpression.GroupBy);
			Assert.AreEqual(1, statement.QueryExpression.GroupBy.Count());
			Assert.IsInstanceOf<SqlReferenceExpression>(statement.QueryExpression.GroupBy.ElementAt(0));

			Assert.IsNotNull(statement.QueryExpression.HavingExpression);
			Assert.IsInstanceOf<SqlBinaryExpression>(statement.QueryExpression.HavingExpression);
		}

		[Test]
		public static void OrderByColumn() {
			var statement = SqlStatementBuilder.Select(select => select
				.Query(query => query.AllColumns()
					.FromTable("app.test"))
					.OrderBy(orderBy => orderBy.Column("name").Descending()));

			Assert.IsNotNull(statement);
			Assert.IsNotNull(statement.QueryExpression);

			Assert.IsNotEmpty(statement.QueryExpression.SelectColumns);
			Assert.AreEqual(1, statement.QueryExpression.SelectColumns.Count());
			Assert.IsTrue(statement.QueryExpression.SelectColumns.ElementAt(0).IsAll);
			Assert.IsNotEmpty(statement.QueryExpression.FromClause.AllTables);
			Assert.AreEqual(1, statement.QueryExpression.FromClause.AllTables.Count());
			Assert.AreEqual("app.test", statement.QueryExpression.FromClause.AllTables.ElementAt(0).Name);

			Assert.IsNotEmpty(statement.OrderBy);
			Assert.AreEqual(1, statement.OrderBy.Count());
			Assert.IsFalse(statement.OrderBy.ElementAt(0).Ascending);
		}
	}
}
