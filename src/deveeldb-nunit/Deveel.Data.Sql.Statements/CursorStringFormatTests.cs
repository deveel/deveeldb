using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CursorStringFormatTests {
		[Test]
		public static void DeclareCursorNoParameters() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT a, b FROM test_table GROUP BY a HAVING b > 3");
			var statement = new DeclareCursorStatement("c1", query);

			var sql = statement.ToString();

			var expected = new SqlStringBuilder();
			expected.AppendLine("CURSOR c1 IS");
			expected.Append("  SELECT a, b FROM test_table GROUP BY a HVAING b > 3");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void DeclareCursorWithParameters() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT a, b FROM test_table WHERE a / 4 = c");
			var statement = new DeclareCursorStatement("c1", new []{new CursorParameter("c", PrimitiveTypes.Integer()), }, query);

			var sql = statement.ToString();

			var expected = new SqlStringBuilder();
			expected.AppendLine("CURSOR c1(c INTEGER) IS");
			expected.Append("  SELECT a, b FROM test_table WHERE a / 4 = c");

			Assert.AreEqual(expected.ToString(), sql);
		}

		[Test]
		public static void OpenCursorNoArguments() {
			var statement = new OpenStatement("c1");

			var sql = statement.ToString();
			var expected = "OPEN c1";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void OpenCursorWithArguments() {
			var statement = new OpenStatement("c1", new SqlExpression[] {
				SqlExpression.Constant(45),
				SqlExpression.VariableReference("a")
			});

			var sql = statement.ToString();
			var expected = "OPEN c1(45, :a)";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void CloseCursor() {
			var statement = new CloseStatement("vector1");

			var sql = statement.ToString();
			var expected = "CLOSE vector1";

			Assert.AreEqual(expected, sql);
		}
	}
}
