using System;

using Deveel.Data.Routines;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public static class SqlFunctionCallExpressionTests {
		[Test]
		public static void FormatWithNoArguments() {
			var expression = SqlExpression.FunctionCall(ObjectName.Parse("SYS.func1"));

			var sql = expression.ToString();
			var expected = "SYS.func1()";
			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void FormatWithAnonArguments() {
			var expression = SqlExpression.FunctionCall(ObjectName.Parse("SYS.func1"), new SqlExpression[] {
				SqlExpression.Constant("order1"),
				SqlExpression.Constant(2)
			});

			var sql = expression.ToString();
			var expected = "SYS.func1('order1', 2)";
			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void FormatWithNamedArguments() {
			var expression = SqlExpression.FunctionCall(ObjectName.Parse("SYS.func1"), new InvokeArgument[] {
				new InvokeArgument("a", SqlExpression.Constant(1)), 
			});

			var sql = expression.ToString();
			var expected = "SYS.func1(a => 1)";
			Assert.AreEqual(expected, sql);

		}
	}
}
