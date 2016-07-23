using System;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class DeleteStringFormatTests {
		[Test]
		public static void SimpleDelete() {
			var condition = SqlExpression.GreaterThan(SqlExpression.Reference(new ObjectName("id")), SqlExpression.Constant(22));
			var statement = new DeleteStatement(ObjectName.Parse("t1"), condition);

			var sql = statement.ToString();
			var expected = "DELETE FROM t1 WHERE id > 22";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void Limited() {
			var condition = SqlExpression.SmallerThan(SqlExpression.Reference(new ObjectName("code")), SqlExpression.Constant(1002));
			var statement = new DeleteStatement(ObjectName.Parse("t1"), condition);
			statement.Limit = 10;

			var sql = statement.ToString();
			var expected = "DELETE FROM t1 WHERE code < 1002 LIMIT 10";

			Assert.AreEqual(expected, sql);
		}
	}
}
