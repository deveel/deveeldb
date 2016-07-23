using System;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class SelectStringFormatTests {
		[Test]
		public static void SimpleAll() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM t1");
			var statement = new SelectStatement(query);

			var sql = statement.ToString();
			var expected = "SELECT * FROM t1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void AllWithJoin() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM t1, t2 WHERE t1.id = t2.other_id");
			var statement = new SelectStatement(query);

			var sql = statement.ToString();
			var expected = "SELECT * FROM t1, t2 WHERE t1.id = t2.other_id";
			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void AllWithInnerJoint() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.other_id");
			var statement = new SelectStatement(query);

			var sql = statement.ToString();
			var expected = "SELECT * FROM table1 AS t1 INNER JOIN table2 AS t2 ON t1.id = t2.other_id";

			Assert.AreEqual(expected, sql);
		}
	}
}
