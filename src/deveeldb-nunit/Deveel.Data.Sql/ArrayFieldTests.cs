using System;

using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class ArrayFieldTests {
		[Test]
		public void AllEqualTo() {
			var array = Field.Array(new SqlExpression[] {
				SqlExpression.Constant(23),
				SqlExpression.Add(SqlExpression.Constant(12), SqlExpression.Constant(11)),
				SqlExpression.Add(SqlExpression.Multiply(SqlExpression.Constant(5), SqlExpression.Constant(4)), SqlExpression.Constant(3))
			});

			var value = Field.Integer(23);
			var result = value.All(SqlExpressionType.Equal, array, new EvaluateContext(null, null));

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.IsTrue(result.Is(Field.BooleanTrue));
		}

		[Test]
		public void AnyEqualTo() {
			var array = Field.Array(new SqlExpression[] {
				SqlExpression.Constant(23),
				SqlExpression.Add(SqlExpression.Constant(51), SqlExpression.Constant(11)),
				SqlExpression.Add(SqlExpression.Divide(SqlExpression.Constant(50), SqlExpression.Constant(4)), SqlExpression.Constant(3))
			});

			var value = Field.Integer(23);
			var result = value.Any(SqlExpressionType.Equal, array, new EvaluateContext(null, null));

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.IsTrue(result.Is(Field.BooleanTrue));

		}
	}
}
