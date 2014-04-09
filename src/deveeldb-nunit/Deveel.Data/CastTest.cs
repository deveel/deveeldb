using System;

using Deveel.Data.Sql;
using Deveel.Data.Types;
using Deveel.Math;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class CastTest {
		[TestCase("1980-10-06")]
		[TestCase("2001 11 22")]
		public void DateCast(object data) {
			TObject obj = TObject.CreateAndCastFromObject(TType.GetDateType(SqlType.Date), data);
			Assert.IsNotNull(obj);
			Assert.AreEqual(obj.TType.SqlType, SqlType.Date);
		}

		[TestCase("22:58:10.578")]
		[TestCase("15:27:32")]
		[TestCase("14:08:35 +2")]
		[TestCase("19:02:11.478 +12")]
		// TODO: Test against ticks
		public void TimeCast(object data) {
			TObject obj = TObject.CreateAndCastFromObject(TType.GetDateType(SqlType.Time), data);
			Assert.IsNotNull(obj);
			Assert.AreEqual(obj.TType.SqlType, SqlType.Time);
		}

		[TestCase("1957-05-12T22:41:36.735 +5")]
		[TestCase("2015-03-18T14:35:01.447")]
		[TestCase("1478-01-01T17:10:05")]
		[TestCase("1980-04-06 03:12:00")]
		[TestCase("1957-12-01 07:18:14.556 +11")]
		public void TimeStampCast(string data) {
			TObject obj = TObject.CreateAndCastFromObject(TType.GetDateType(SqlType.TimeStamp), data);
			Assert.IsNotNull(obj);
			Assert.AreEqual(obj.TType.SqlType, SqlType.TimeStamp);
		}

		[TestCase("true", true)]
		[TestCase("false", false)]
		[TestCase("0", false)]
		[TestCase("1", true)]
		[TestCase("True", true)]
		public void CastToBoolean(object data, bool expected) {
			TObject obj = TObject.CreateAndCastFromObject(TType.GetBooleanType(SqlType.Boolean), data);
			Assert.IsNotNull(obj);
			Assert.AreEqual(obj.TType.SqlType, SqlType.Boolean);
			Assert.AreEqual(expected, obj.Object);
		}
	}
}
