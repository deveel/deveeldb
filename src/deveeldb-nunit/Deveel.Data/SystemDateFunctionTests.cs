using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SystemDateFunctionTests : FunctionTestBase {
		[Test]
		public void CurrentDate() {
			var value = Select("DATE");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void CurrentTime() {
			var value = Select("TIME");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void CurrentTimeStamp() {
			var value = Select("TIMESTAMP");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemDate() {
			var value = Select("SYSTEM_DATE");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemTime() {
			var value = Select("SYSTEM_TIME");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void SystemTimeStamp() {
			var value = Select("SYSTEM_TIMESTAMP");

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// It is impossible to determine the value of the current date 
			// in this test scenario
		}

		[Test]
		public void AddDate_Year() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("year"),
				SqlExpression.Constant(15));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void AddDate_Month() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("month"),
				SqlExpression.Constant(2));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void AddDate_Day() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("day"),
				SqlExpression.Constant(2));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void AddDate_Hour() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("hour"),
				SqlExpression.Constant(27));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void AddDate_Minute() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("minute"),
				SqlExpression.Constant(2));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void AddDate_Second() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("ADD_DATE", SqlExpression.Constant(date), SqlExpression.Constant("second"),
				SqlExpression.Constant(2));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}

		[Test]
		public void Extract_Year() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("EXTRACT", SqlExpression.Constant(date), SqlExpression.Constant("year"));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<NumericType>(value.Type);
			Assert.IsInstanceOf<SqlNumber>(value.Value);

			Assert.AreEqual(new SqlNumber(1980), value.Value);
		}

		[Test]
		public void Extract_Month() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("EXTRACT", SqlExpression.Constant(date), SqlExpression.Constant("month"));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<NumericType>(value.Type);
			Assert.IsInstanceOf<SqlNumber>(value.Value);

			Assert.AreEqual(new SqlNumber(02), value.Value);
		}

		[Test]
		public void Extract_Day() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("EXTRACT", SqlExpression.Constant(date), SqlExpression.Constant("day"));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<NumericType>(value.Type);
			Assert.IsInstanceOf<SqlNumber>(value.Value);

			Assert.AreEqual(new SqlNumber(05), value.Value);
		}


		[Test]
		public void NextDay() {
			var date = new SqlDateTime(1980, 02, 05, 18, 20, 11, 32);
			var value = Select("NEXT_DAY", SqlExpression.Constant(date), SqlExpression.Constant("Wednesday"));

			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.IsInstanceOf<DateType>(value.Type);
			Assert.IsInstanceOf<SqlDateTime>(value.Value);

			// TODO: Assert result
		}
	}
}
