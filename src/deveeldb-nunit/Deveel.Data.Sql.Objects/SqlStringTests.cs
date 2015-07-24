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
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	[Category("SQL Objects")]
	public class SqlStringTests {
		[Test]
		[Category("Strings")]
		public void String_Create() {
			const string s = "Test string UTF-16 LE";
			var sqlString = new SqlString(s);
			Assert.IsNotNull(sqlString);
			Assert.AreEqual(s.Length, sqlString.Length);
			Assert.AreEqual(s, sqlString);
		}

		[Test]
		[Category("Strings")]
		public void String_Compare_Equal() {
			const string s = "Test string in UTF-16 LE";
			var sqlString1 = new SqlString(s);
			var sqlString2 = new SqlString(s);
			Assert.AreEqual(0, sqlString1.CompareTo(sqlString2));
		}

		[Test]
		[Category("Strings")]
		public void String_Equals() {
			const string s = "Test string in UTF-16 LE";
			var sqlString1 = new SqlString(s);
			var sqlString2 = new SqlString(s);
			Assert.IsTrue(sqlString1.Equals(sqlString2));
		}

		[Test]
		[Category("Strings")]
		public void String_Concat() {
			const string s1 = "First string comes before the ";
			const string s2 = "Second string that comes after";
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var sqlString3 = new SqlString();
			Assert.DoesNotThrow(() => sqlString3 = sqlString1.Concat(sqlString2));
			Assert.AreEqual("First string comes before the Second string that comes after", sqlString3.Value);
		}

		[Test]
		public void String_EqualsToNull() {
			var s = SqlString.Null;
			Assert.IsTrue(s.IsNull);
			Assert.AreEqual(SqlNull.Value, s);
			Assert.AreEqual(s, SqlNull.Value);
		}

		[Test]
		[Category("Conversion")]
		[Category("Date Time")]
		public void String_Convert_TimeStamp() {
			const string s = "2011-01-23T23:44:21.525 +01:00";
			var sqlString = new SqlString(s);

			var timeStamp = new SqlDateTime();
			Assert.DoesNotThrow(() => timeStamp = (SqlDateTime) Convert.ChangeType(sqlString, typeof(SqlDateTime)));
			Assert.IsFalse(timeStamp.IsNull);
			Assert.AreEqual(2011, timeStamp.Year);
			Assert.AreEqual(01, timeStamp.Month);
			Assert.AreEqual(23, timeStamp.Day);
			Assert.AreEqual(23, timeStamp.Hour);
			Assert.AreEqual(44, timeStamp.Minute);
			Assert.AreEqual(525, timeStamp.Millisecond);
			Assert.AreEqual(1, timeStamp.Offset.Hours);
			Assert.AreEqual(0, timeStamp.Offset.Minutes);
		}

		[Test]
		[Category("Conversion")]
		[Category("Date Time")]
		public void String_Convert_Time() {
			const string s = "23:44:21.525";
			var sqlString = new SqlString(s);

			var time = new SqlDateTime();
			Assert.DoesNotThrow(() => time = (SqlDateTime) Convert.ChangeType(sqlString, typeof(SqlDateTime)));
			Assert.IsFalse(time.IsNull);
			Assert.AreEqual(1, time.Year);
			Assert.AreEqual(1, time.Month);
			Assert.AreEqual(1, time.Day);
			Assert.AreEqual(23, time.Hour);
			Assert.AreEqual(44, time.Minute);
			Assert.AreEqual(21, time.Second);
			Assert.AreEqual(525, time.Millisecond);
		}

		[Test]
		[Category("Conversion")]
		[Category("Date Time")]
		public void String_Convert_Date() {
			const string s = "2011-01-23";
			var sqlString = new SqlString(s);

			var date = new SqlDateTime();
			Assert.DoesNotThrow(() => date = (SqlDateTime) Convert.ChangeType(sqlString, typeof(SqlDateTime)));
			Assert.IsFalse(date.IsNull);
			Assert.AreEqual(2011, date.Year);
			Assert.AreEqual(01, date.Month);
			Assert.AreEqual(23, date.Day);
			Assert.AreEqual(0, date.Hour);
			Assert.AreEqual(0, date.Minute);
			Assert.AreEqual(0, date.Millisecond);
			Assert.AreEqual(0, date.Offset.Hours);
			Assert.AreEqual(0, date.Offset.Minutes);			
		}

		[Test]
		[Category("Conversion")]
		[Category("Numbers")]
		public void String_Convert_BigNumber() {
			const string s = "7689994.0000033992988477226661525553666370058812345883288477383";
			var sqlString = new SqlString(s);

			var number = new SqlNumber();
			Assert.DoesNotThrow(() => number = (SqlNumber)Convert.ChangeType(sqlString, typeof(SqlNumber)));
			Assert.IsFalse(number.IsNull);
			Assert.IsFalse(number.CanBeInt32);
			Assert.IsFalse(number.CanBeInt64);
			Assert.AreEqual(NumericState.None, number.State);
		}

		[Test]
		[Category("Conversion")]
		[Category("Booleans")]
		public void String_Convert_BooleanTrue() {
			const string s = "true";
			var sqlString = new SqlString(s);

			var b = new SqlBoolean();
			Assert.DoesNotThrow(() => b = (SqlBoolean)Convert.ChangeType(sqlString, typeof(SqlBoolean)));
			Assert.IsFalse(b.IsNull);
			Assert.AreEqual(SqlBoolean.True, b);
		}

		[Test]
		[Category("Conversion")]
		[Category("Booleans")]
		public void String_Convert_BooleanFalse() {
			const string s = "false";
			var sqlString = new SqlString(s);

			var b = new SqlBoolean();
			Assert.DoesNotThrow(() => b = (SqlBoolean)Convert.ChangeType(sqlString, typeof(SqlBoolean)));
			Assert.IsFalse(b.IsNull);
			Assert.AreEqual(SqlBoolean.False, b);
		}

		[Test]
		[Category("Conversion")]
		[Category("Booleans")]
		public void String_Convert_BooleanNull() {
			const string s = "";
			var sqlString = new SqlString(s);

			var b = new SqlBoolean();
			Assert.DoesNotThrow(() => b = (SqlBoolean)Convert.ChangeType(sqlString, typeof(SqlBoolean)));
			Assert.IsTrue(b.IsNull);
			Assert.AreEqual(SqlBoolean.Null, b);
		}
	}
}
