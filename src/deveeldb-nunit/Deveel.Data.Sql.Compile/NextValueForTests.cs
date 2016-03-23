using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class NextValueForTests : SqlCompileTestBase {
		[Test]
		public void Simple() {
			const string sql = "SELECT NEXT VALUE FOR app.seq1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement) statement;
			Assert.IsNotNull(select.QueryExpression);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());

			var col1 = select.QueryExpression.SelectColumns.ElementAt(0);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(col1.Expression);
		}
	}
}
