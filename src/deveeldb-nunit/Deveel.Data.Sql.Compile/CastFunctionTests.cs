using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CastFunctionTests : SqlCompileTestBase {
		[Test]
		public void CastNumberToVarchar() {
			const string sql = "SELECT CAST(8763 AS VARCHAR)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<SelectStatement>(statement);

			var select = (SelectStatement) statement;

			Assert.IsNotNull(select.QueryExpression.SelectColumns);
			Assert.AreEqual(1, select.QueryExpression.SelectColumns.Count());
		}
	}
}
