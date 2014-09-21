// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	[Category("Functions")]
	public class SystemFunctionsTests : SqlTestBase {
		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Concat_PlainText() {
			var result = ExecuteScalar("SELECT CONCAT('The', ' ', 'Brown', ' ', 'Quick', ' ', 'Fox')");

			Assert.IsNotNull(result);
			Assert.AreEqual("The Brown Quick Fox", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Replace_PlainText() {
			var result = ExecuteScalar("SELECT REPLACE('The quick brown duck','duck', 'fox')");

			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Substring_TwoArgs_PlainText() {
			var result = ExecuteScalar("SELECT SUBSTRING('The quick brown fox', 5)");

			Assert.IsNotNull(result);
			Assert.AreEqual("quick brown fox", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Substring_ThreeArgs_PlainText() {
			var result = ExecuteScalar("SELECT SUBSTRING('The quick brown fox', 5, 11)");

			Assert.IsNotNull(result);
			Assert.AreEqual("quick brown", result.ToString());			
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void InString_TwoArgs_PlainText() {
			var result = ExecuteScalarToInt64("SELECT INSTR('The quick brown fox', 'brown')");

			Assert.IsNotNull(result);
			Assert.AreEqual(11, result);
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void InString_ThreeArgs_PlainText() {
			var result = ExecuteScalarToInt64("SELECT INSTR('The quick brown fox', 'brown', 2)");

			Assert.IsNotNull(result);
			Assert.AreEqual(11, result);
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void LPad_TwoArgs_PlainText() {
			var result = ExecuteScalar("SELECT LPAD('The quick brown fox', 23)");

			Assert.IsNotNull(result);
			Assert.AreEqual("    The quick brown fox", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void LPad_ThreeArgs_PlainText() {
			var result = ExecuteScalar("SELECT LPAD('The quick brown fox', 23, '_')");

			Assert.IsNotNull(result);
			Assert.AreEqual("____The quick brown fox", result.ToString());			
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void RPad_TwoArgs_PlainText() {
			var result = ExecuteScalar("SELECT RPAD('The quick brown fox', 23)");

			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox    ", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void RPad_ThreeArgs_PlainText() {
			var result = ExecuteScalar("SELECT RPAD('The quick brown fox', 23, '_')");

			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox____", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Soundex_PlainText() {
			var result = ExecuteScalar("SELECT SOUNDEX('The quick brown fox')");
			
			Assert.IsNotNull(result);
			Assert.AreEqual("T221", result.ToString());
		}

		[Test]
		[Category("String")]
		public void Lower_PlainText() {
			var result = ExecuteScalar("SELECT LOWER('The Quick BroWn foX')");

			Assert.IsNotNull(result);
			Assert.AreEqual("the quick brown fox", result.ToString());
		}

		[Test]
		[Category("String")]
		public void Upper_PlainText() {
			var result = ExecuteScalar("SELECT UPPER('The quick brOwn Fox')");

			Assert.IsNotNull(result);
			Assert.AreEqual("THE QUICK BROWN FOX", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void Trim_PlainText() {
			var result = ExecuteScalar("SELECT TRIM(LEADING ' ' FROM '  The quick brown fox')");

			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox", result.ToString());
		}

		[Test]
		[Category("Functions")]
		[Category("String")]
		public void RTrim_PlainText() {
			var result = ExecuteScalar("SELECT RTRIM('The quick brown fox   ')");
			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox", result.ToString());

			result = ExecuteScalar("SELECT RTRIM('The quick brown fox&&&%', '&%')");
			Assert.IsNotNull(result);
			Assert.AreEqual("The quick brown fox", result.ToString());
		}

		[Test]
		[Category("Strings")]
		public void CharLength() {
			var result = ExecuteScalarToInt64("SELECT CHAR_LENGTH('The quick brown fox')");
			Assert.IsNotNull(result);
			Assert.AreEqual(19, result);
		}

		[Test]
		[Category("Functions")]
		[Category("Date")]
		public void DateParseFunction_PlainText() {
			var result = ExecuteScalar("SELECT DATE '1980-06-04'");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1980, 06, 04), result);
		}

		[Test]
		[Category("Date")]
		public void DateParseFunction_Function() {
			var result = ExecuteScalar("SELECT DATEOB('1980-06-04')");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1980, 06, 04), result);
		}

		[Test]
		[Category("Functions")]
		[Category("Date")]
		public void CurrentDateFunction() {
			var result = ExecuteScalar("SELECT CURRENT_DATE");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(DateTime.Today, result);
		}

		[Test]
		[Category("Date")]
		public void TimeParseFunction_PlainText() {
			var result = ExecuteScalar("SELECT TIME '04:25:16'");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1, 1, 1, 4, 25, 16), result);
		}

		[Test]
		[Category("Date")]
		public void TimeParseFunction_Function() {
			var result = ExecuteScalar("SELECT TIMEOB('04:25:16')");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1, 1, 1, 4, 25, 16), result);
		}

		[Test]
		[Category("Date")]
		public void CurrentTimeFunction() {
			var result = ExecuteScalar("SELECT CURRENT_TIME");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
		}

		[Test]
		[Category("Date")]
		public void TimeSpanParse_PlainText() {
			var result = ExecuteScalar("SELECT TIMESTAMP '1980-06-04T02:35:00'");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1980, 06, 04, 2, 35, 0), result);
		}

		[Test]
		[Category("Date")]
		public void TimeSpanParse_Function() {
			var result = ExecuteScalar("SELECT TIMESTAMPOB('1980-06-04T02:35:00')");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
			Assert.AreEqual(new DateTime(1980, 06, 04, 2, 35, 0), result);
		}

		[Test]
		[Category("Date")]
		public void CurrentTimeStampFunction() {
			var result = ExecuteScalar("SELECT CURRENT_TIMESTAMP");
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<DateTime>(result);
		}

		[Test]
		[Category("Date")]
		public void DbTimeZone_Static() {
			var result = ExecuteScalar("SELECT DBTIMEZONE");
			Assert.IsNotNull(result);
			Assert.AreEqual(TimeZone.CurrentTimeZone.StandardName, result.ToString());
		}

		[Test]
		[Category("Security")]
		public void CurrentUser() {
			var result = ExecuteScalar("SELECT USER()");
			Assert.IsNotNull(result);
			Assert.AreEqual(AdminUser, result.ToString());
		}

		[Test]
		public void Exists_StaticSelect() {
			Assert.Ignore("Need to define a structure to select from");
		}

		[Test]
		public void Coalesce_Static() {
			var result = ExecuteScalar("SELECT COALESCE(NULL, 'A quick brown duck', 34, NULL, 0.988)");
			Assert.IsNotNull(result);
			Assert.AreEqual("A quick brown duck", result.ToString());
		}

		[Test]
		public void Greatest_Static() {
			var result = ExecuteScalar("SELECT GREATEST(25, 0.58, 14.665, NULL)");
			Assert.IsNotNull(result);
			Assert.AreEqual(DBNull.Value, result);

			result = ExecuteScalar("SELECT GREATEST(47, 105.87, 68)");
			Assert.IsNotNull(result);
			Assert.IsTrue(((BigNumber)result).CompareTo(105.87) == 0);
		}

		[Test]
		[Category("Math")]
		public void Abs_Small() {
			var result = ExecuteScalarToDouble("SELECT ABS(-9871)");
			Assert.AreEqual(9871, result);

			result = ExecuteScalarToDouble("SELECT ABS(651.89)");
			Assert.AreEqual(651.89, result);

			result = ExecuteScalarToDouble("SELECT ABS(-167.98)");
			Assert.AreEqual(167.98, result);
		}

		[Test]
		[Category("Math")]
		public void Abs_Big() {
			var result = ExecuteScalar("SELECT ABS(-7.4723342916989754E+45)");
			Assert.AreEqual(BigNumber.Parse("-7.4723342916989754E+45").Abs(), result);
		}
	}
}