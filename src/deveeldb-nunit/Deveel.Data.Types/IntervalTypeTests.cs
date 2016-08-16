using System;
using System.IO;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class IntervalTypeTests {
		[TestCase("INTERVAL DAY TO SECOND", SqlTypeCode.DayToSecond)]
		[TestCase("INTERVAL YEAR TO MONTH", SqlTypeCode.YearToMonth)]
		public void ParseString(string input, SqlTypeCode expected) {
			var type = SqlType.Parse(input);

			Assert.IsNotNull(type);
			Assert.IsInstanceOf<IntervalType>(type);

			Assert.AreEqual(expected, type.TypeCode);
		}

		[Test]
		public void FormatString_DayToSecond() {
			var interval = PrimitiveTypes.DayToSecond();

			var s = interval.ToString();
			Assert.AreEqual("INTERVAL DAY TO SECOND", s);
		}

		[Test]
		public void FormatString_YearToMonth() {
			var interval = PrimitiveTypes.YearToMonth();

			var s = interval.ToString();
			Assert.AreEqual("INTERVAL YEAR TO MONTH", s);
		}

		[Test]
		public void SerializeObject_DayToSecond_NotNull() {
			var type = PrimitiveTypes.DayToSecond();
			var obj = new SqlDayToSecond(22, 4, 15, 0);

			var stream = new MemoryStream();
			type.SerializeObject(stream, obj);

			stream.Seek(0, SeekOrigin.Begin);

			var serialized = type.DeserializeObject(stream);

			Assert.AreEqual(obj, serialized);
		}

		[Test]
		public void SerializeObject_DayToSecond_Null() {
			var type = PrimitiveTypes.DayToSecond();
			var obj = SqlDayToSecond.Null;

			var stream = new MemoryStream();
			type.SerializeObject(stream, obj);

			stream.Seek(0, SeekOrigin.Begin);

			var serialized = type.DeserializeObject(stream);

			Assert.AreEqual(obj, serialized);
		}

		[Test]
		public void SerializeObject_YearToMonth_NotNull() {
			var type = PrimitiveTypes.YearToMonth();
			var obj = new SqlYearToMonth(45);

			var stream = new MemoryStream();
			type.SerializeObject(stream, obj);

			stream.Seek(0, SeekOrigin.Begin);

			var serialized = type.DeserializeObject(stream);

			Assert.AreEqual(obj, serialized);
		}

		[Test]
		public void SerializeObject_YearToMonth_Null() {
			var type = PrimitiveTypes.YearToMonth();
			var obj = SqlYearToMonth.Null;

			var stream = new MemoryStream();
			type.SerializeObject(stream, obj);

			stream.Seek(0, SeekOrigin.Begin);

			var serialized = type.DeserializeObject(stream);

			Assert.AreEqual(obj, serialized);
		}

		[TestCase(SqlTypeCode.YearToMonth, typeof(SqlYearToMonth))]
		[TestCase(SqlTypeCode.DayToSecond, typeof(SqlDayToSecond))]
		public void GetObjectType(SqlTypeCode code, Type expectedType) {
			var type = PrimitiveTypes.Interval(code);
			var objType = type.GetObjectType();

			Assert.AreEqual(expectedType, objType);
		}

		[TestCase(SqlTypeCode.YearToMonth, typeof(long))]
		[TestCase(SqlTypeCode.DayToSecond, typeof(TimeSpan))]
		public void GetRuntimeType(SqlTypeCode code, Type expectedType) {
			var type = PrimitiveTypes.Interval(code);
			var objType = type.GetRuntimeType();

			Assert.AreEqual(expectedType, objType);
		}
	}
}
