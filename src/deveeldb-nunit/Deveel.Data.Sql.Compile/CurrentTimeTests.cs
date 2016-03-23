using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CurrentTimeTests : SqlCompileTestBase {
		[Test]
		public void CurrentTime() {
			const string sql = "SELECT CURRENT_TIME";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;
			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());

			var col1 = select.QueryExpression.SelectColumns.ElementAt(0);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(col1.Expression);
		}

		[Test]
		public void CurrentTimeStamp() {
			const string sql = "SELECT CURRENT_TIMESTAMP";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;
			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());

			var col1 = select.QueryExpression.SelectColumns.ElementAt(0);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(col1.Expression);
		}

		[Test]
		public void CurrentDate() {
			const string sql = "SELECT CURRENT_DATE";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement)statement;
			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());

			var col1 = select.QueryExpression.SelectColumns.ElementAt(0);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(col1.Expression);

		}
	}
}
