using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlCastExpressionTests {
		[Test]
		public void CastStringToInteger() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("1234")), PrimitiveTypes.Integer());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression) casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<NumericType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Integer, value.Type.SqlType);
			Assert.AreEqual(new SqlNumber(1234), value.Value);
		}

		[Test]
		public void CastStringToNumber() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("12.3e4")), PrimitiveTypes.Numeric());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<NumericType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Numeric, value.Type.SqlType);
			Assert.AreEqual(SqlNumber.Parse("12.3e4"), value.Value);
		}

		[Test]
		public void CastStringToDateTime() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("2015-09-01")), PrimitiveTypes.DateTime());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.AreEqual(SqlTypeCode.DateTime, value.Type.SqlType);
			Assert.AreEqual(new SqlDateTime(2015, 09, 01), value.Value);
		}

		[Test]
		public void CastStringToDate() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("2015-09-01")), PrimitiveTypes.Date());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Date, value.Type.SqlType);
			Assert.AreEqual(new SqlDateTime(2015, 09, 01), value.Value);
		}

		[Test]
		public void CastStringToTime() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("22:13:01")), PrimitiveTypes.Time());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Time, value.Type.SqlType);

			// we round the expected value to the result offset because of the UTC parsing logic
			// of the date type: all we care here is the time component

			var result = ((SqlDateTime)value.Value);
			var expected = new SqlDateTime(1, 1, 1, 22, 13, 01, 0, result.Offset);
			Assert.AreEqual(expected, result);
		}


		[Test]
		public void CastStringToBooleanTrue() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("true")), PrimitiveTypes.Boolean());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<BooleanType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Boolean, value.Type.SqlType);
			Assert.AreEqual(SqlBoolean.True, value.Value);
		}

		[Test]
		public void CastStringToBooleanFalse() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.String("false")), PrimitiveTypes.Boolean());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<BooleanType>(value.Type);
			Assert.AreEqual(SqlTypeCode.Boolean, value.Type.SqlType);
			Assert.AreEqual(SqlBoolean.False, value.Value);
		}

		[Test]
		public void CastBooleanFalseToString() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.Boolean(false)), PrimitiveTypes.String());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<StringType>(value.Type);
			Assert.AreEqual(SqlTypeCode.String, value.Type.SqlType);
			Assert.AreEqual(SqlString.Unicode("False"), value.Value);
		}

		[Test]
		public void CastBooleanTrueToString() {
			var exp = SqlExpression.Cast(SqlExpression.Constant(DataObject.Boolean(true)), PrimitiveTypes.String());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<StringType>(value.Type);
			Assert.AreEqual(SqlTypeCode.String, value.Type.SqlType);
			Assert.AreEqual(SqlString.Unicode("True"), value.Value);
		}

		[Test]
		public void CastDateToString() {
			var date = DataObject.Date(new SqlDateTime(2015, 02, 03));
			var exp = SqlExpression.Cast(SqlExpression.Constant(date), PrimitiveTypes.String());

			SqlExpression casted = null;
			Assert.DoesNotThrow(() => casted = exp.Evaluate());
			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlConstantExpression>(casted);

			var value = ((SqlConstantExpression)casted).Value;
			Assert.IsNotNull(value.Value);
			Assert.IsInstanceOf<StringType>(value.Type);
			Assert.AreEqual(SqlTypeCode.String, value.Type.SqlType);
			Assert.AreEqual(SqlString.Unicode("2015-02-03"), value.Value);
		}

		[Test]
		public void CastNumberToString() {
			Assert.Inconclusive();
		}
	}
}
