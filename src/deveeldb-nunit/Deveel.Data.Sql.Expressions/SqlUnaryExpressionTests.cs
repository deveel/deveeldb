using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlUnaryExpressionTests {
		[TestCase(4637377, 4637377)]
		[TestCase(-1929934, -1929934)]
		public void NumericPlus(double a, double expected) {
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(a)));
			var plusExp = SqlExpression.UnaryPlus(exp1);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = plusExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value);
			var expectedResult = new SqlNumber(expected);
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(884920009.9948, -884920009.9948)]
		[TestCase(-92338.122, 92338.122)]
		public void NumericNegate(double a, double expected) {
			var exp1 = SqlExpression.Constant(DataObject.Number(new SqlNumber(a)));
			var negExp = SqlExpression.Negate(exp1);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = negExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<NumericType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlNumber>(constExp.Value.Value);

			var actual = ((SqlNumber)constExp.Value.Value);
			var expectedResult = new SqlNumber(expected);
			Assert.AreEqual(expectedResult, actual);
		}

		[TestCase(true, false)]
		[TestCase(false, true)]
		public void BooleanNegate(bool a, bool expected) {
			var exp1 = SqlExpression.Constant(DataObject.Boolean(a));
			var negExp = SqlExpression.Negate(exp1);

			SqlExpression resultExp = null;
			Assert.DoesNotThrow(() => resultExp = negExp.Evaluate());
			Assert.IsNotNull(resultExp);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExp);

			var constExp = (SqlConstantExpression)resultExp;
			Assert.IsNotNull(constExp.Value.Value);
			Assert.IsInstanceOf<BooleanType>(constExp.Value.Type);
			Assert.IsInstanceOf<SqlBoolean>(constExp.Value.Value);

			var actual = ((SqlBoolean)constExp.Value.Value);
			var expectedResult = new SqlBoolean(expected);
			Assert.AreEqual(expectedResult, actual);
		}
	}
}
