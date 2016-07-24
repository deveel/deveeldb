using System;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateViewStringFormatTests {
		[Test]
		public static void WithNoColumns() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test1 WHERE a > 2");
			var statement = new CreateViewStatement(ObjectName.Parse("APP.view1"), query);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.AppendLine("CREATE VIEW APP.view1 IS");
			expected.Append("  SELECT * FROM test1 WHERE a > 2");
			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void WithColumns() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT id AS a, name AS b FROM test1 WHERE a > 2");
			var statement = new CreateViewStatement(ObjectName.Parse("APP.view1"), new []{"a", "b"}, query);

			var sql = statement.ToString();
			var expected = new SqlStringBuilder();
			expected.AppendLine("CREATE VIEW APP.view1(a, b) IS");
			expected.Append("  SELECT id AS a, name AS b FROM test1 WHERE a > 2");
			Assert.AreEqual(expected.ToString(), sql);
		}
	}
}
