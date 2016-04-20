using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	[Category("Data Types")]
	public sealed class DateTypeTests {
		[TestCase("DATE", SqlTypeCode.Date)]
		[TestCase("DATETIME", SqlTypeCode.DateTime)]
		[TestCase("TIME", SqlTypeCode.Time)]
		[TestCase("TIMESTAMP", SqlTypeCode.TimeStamp)]
		public void ParseString(string input, SqlTypeCode typeCode) {
			var sqlType = SqlType.Parse(input);

			Assert.IsNotNull(sqlType);
			Assert.IsInstanceOf<DateType>(sqlType);
			Assert.AreEqual(typeCode, sqlType.TypeCode);
		}

		[TestCase(2014, 03, 05, 22, 18, 09, 0, "2014-03-05T22:18:09.000 +00:00")]
		[TestCase(2014, 03, 05, 22, 18, 09, 451, "2014-03-05T22:18:09.451 +00:00")]
		public void CastDateTimeToString(int year, int month, int day, int hour, int minute, int second, int millis, string expected) {
			var type = PrimitiveTypes.DateTime();
			var date = new SqlDateTime(year, month, day, hour, minute, second, millis);

			var casted = type.CastTo(date, PrimitiveTypes.String());

			Assert.IsNotNull(casted);
			Assert.IsInstanceOf<SqlString>(casted);
			
			Assert.AreEqual(expected, casted.ToString());
		}
	}
}
