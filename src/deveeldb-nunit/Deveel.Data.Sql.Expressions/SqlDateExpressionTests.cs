using System;

using Deveel.Data.Sql.Objects;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlDateExpressionTests {
		[Test]
		public void DateSmallerThanOtherDate() {
			const string text = "TODATE('2013-03-01') < TODATE('2013-05-02')";

			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse(text));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlBinaryExpression>(expression);

			SqlExpression evaluated = null;
			Assert.DoesNotThrow(() => evaluated = expression.Evaluate());
			Assert.IsNotNull(evaluated);

			Assert.IsInstanceOf<SqlConstantExpression>(evaluated);

			var value = ((SqlConstantExpression) evaluated).Value;

			Assert.IsInstanceOf<SqlBoolean>(value.Value);

			Assert.AreEqual(SqlBoolean.True, value.Value);
		}
	}
}
