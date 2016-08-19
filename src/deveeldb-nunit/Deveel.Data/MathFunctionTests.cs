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

			Assert.AreEqual(1.0655028755774869, number);
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

			Assert.AreEqual(1.0659007887179619, number);
		}

		[Test]
		public void Abs() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(-45636.0003922)));

			var result = Select("ABS", value);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(45636.0003922, number);
		}

		[Test]
		public void Tan() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(559604.003100)));

			var result = Select("TAN", value);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(23.625265230100791, number);
		}

		// TODO: Function overloads are not supported yet...
		//[Test]
		//public void Round() {
		//	var value = SqlExpression.Constant(Field.Number(new SqlNumber(929928.00111992934)));

		//	var result = Select("ROUND", value);

		//	Assert.IsNotNull(result);
		//	Assert.IsFalse(Field.IsNullField(result));

		//	Assert.IsInstanceOf<NumericType>(result.Type);
		//	Assert.IsInstanceOf<SqlNumber>(result.Value);

		//	var number = ((SqlNumber)result.Value).ToDouble();

		//	Assert.AreEqual(929928.00111992937, number);
		//}

		[Test]
		public void RoundWithPrecision() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(929928.00111992934)));
			var precision = SqlExpression.Constant(Field.Integer(10));

			var result = Select("ROUND", value, precision);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(929928.0011, number);
		}

		[Test]
		public void TanH() {
			var value = SqlExpression.Constant(Field.Number(new SqlNumber(89366647.992)));

			var result = Select("TANH", value);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var number = ((SqlNumber)result.Value).ToDouble();

			Assert.AreEqual(1, number);
		}
	}
}
