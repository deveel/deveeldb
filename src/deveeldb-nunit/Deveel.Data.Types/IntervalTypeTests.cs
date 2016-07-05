using System;

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
	}
}
