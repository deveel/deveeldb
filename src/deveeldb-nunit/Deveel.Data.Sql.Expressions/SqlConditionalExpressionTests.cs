using System;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public static class SqlConditionalExpressionTests {
		[Test]
		public static void SimpleCase() {
			var first = SqlExpression.Constant(34);
			var second = SqlExpression.Add(SqlExpression.Constant(10), SqlExpression.Constant(24));
			var condition = SqlExpression.Equal(first, second);
			var returnExpression = SqlExpression.Constant("It was equal");
			var conditional = SqlExpression.Conditional(condition, returnExpression);

			var result = conditional.EvaluateToConstant(null, null);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.AreEqual("It was equal", result.Value.ToString());
		}

		[Test]
		public static void SimpleCaseWithFalse() {
			var first = SqlExpression.Constant(34);
			var second = SqlExpression.Add(SqlExpression.Constant(10), SqlExpression.Constant(34));
			var condition = SqlExpression.Equal(first, second);
			var ifTrue = SqlExpression.Constant("It was equal");
			var ifFalse = SqlExpression.Constant("It was not equal");

			var conditional = SqlExpression.Conditional(condition, ifTrue, ifFalse);

			var result = conditional.EvaluateToConstant(null, null);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.AreEqual("It was not equal", result.Value.ToString());
		}

		[Test]
		public static void CaseNestedFalse() {
			var first = SqlExpression.Constant(34);
			var second = SqlExpression.Add(SqlExpression.Constant(10), SqlExpression.Constant(34));
			var condition = SqlExpression.Equal(first, second);
			var ifTrue = SqlExpression.Constant("It was equal");
			var ifFalseReturn = SqlExpression.Constant("It was not equal");
			var ifFalse = SqlExpression.Conditional(SqlExpression.Constant(true), ifFalseReturn);

			var conditional = SqlExpression.Conditional(condition, ifTrue, ifFalse);

			var result = conditional.EvaluateToConstant(null, null);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.AreEqual("It was not equal", result.Value.ToString());
		}

		[Test]
		public static void FormatToString() {
			var trueExp = SqlExpression.FunctionCall("testResult");
			var falseExp = SqlExpression.Constant(false);
			var testExp = SqlExpression.Equal(SqlExpression.VariableReference("a"), SqlExpression.Constant(22));
			var condition = SqlExpression.Conditional(testExp, trueExp, falseExp);

			var expected = "CASE WHEN :a = 22 THEN testResult() ELSE FALSE";

			Assert.AreEqual(expected, condition.ToString());
		}
	}
}
