using System;

using Deveel.Data.Sql.Parser;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public abstract class SqlExpressionTestBase {
		[TestFixtureSetUp]
		public void FixtureSetUp() {
			if (SqlParsers.PlSql == null)
				SqlParsers.Expression = new SqlDefaultParser(new SqlExpressionGrammar());
		}

		[TestFixtureTearDown]
		public void FixtureTearDown() {
			SqlParsers.Expression = null;
		}
	}
}
