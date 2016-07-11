using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class MathFunctionTests : FunctionTestBase {
		[Test]
		public void Cos() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(9963)));

			var result = Select("COS", value);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber) result.Value).ToDouble();

			Assert.AreEqual(-0.53211858514845722, number);
		}

		[Test]
		public void CosH() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(0.36f)));

			var result = Select("COSH", value);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(1.0655028755774867, number);
		}

		[Test]
		public void Log() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(99820)));
			var newBase = SqlExpression.Constant(Field.Number(new SqlNumber(48993)));

			var result = Select("LOG", value, newBase);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(1.0659007887179623, number);
		}
	}
}
