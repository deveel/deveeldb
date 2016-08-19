using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	public static class SqlNullTests {
		[Test]
		public static void ConvertToBoolean() {
			bool value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToBoolean(SqlNull.Value));
		}

		[Test]
		public static void ConvertToInt16() {
			short value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToInt16(SqlNull.Value));
		}


		[Test]
		public static void ConvertToInt32() {
			int value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToInt32(SqlNull.Value));
		}

		[Test]
		public static void ConvertToInt64() {
			long value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToInt64(SqlNull.Value));
		}

		[Test]
		public static void ConvertToDouble() {
			double value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToDouble(SqlNull.Value));
		}

		[Test]
		public static void ConvertToChar() {
			char value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToChar(SqlNull.Value));
		}

		[Test]
		public static void ConvertToDateTime() {
			DateTime value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ToDateTime(SqlNull.Value));
		}

		[Test]
		public static void ConvertToOther() {
			object value;
			Assert.Throws<InvalidCastException>(() => value = Convert.ChangeType(SqlNull.Value, typeof(int)));
		}

		[Test]
		public static void CompareToNull() {
			var value1 = SqlNull.Value;
			var value2 = SqlNull.Value;

			int result;
			Assert.Throws<NotSupportedException>(() => result = (value1 as IComparable).CompareTo(value2));
		}

		[Test]
		public static void IsComparableToOther() {
			var value1 = SqlNull.Value;
			var value2 = SqlNull.Value;

			var result = (value1 as ISqlObject).IsComparableTo(value2);

			Assert.IsFalse(result);
		}

		[Test]
		public static void EqualsToNull() {
			var value1 = SqlNull.Value;
			var value2 = SqlNumber.Null;

			var result = value1 == value2;

			Assert.IsTrue(result);
		}

		[Test]
		public static void EqualsToNotNull() {
			var value1 = SqlNull.Value;
			var value2 = new SqlNumber(340.09);

			var result = value1 == value2;

			Assert.IsFalse(result);
		}
	}
}
