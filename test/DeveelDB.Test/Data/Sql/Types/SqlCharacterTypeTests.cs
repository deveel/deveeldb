// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Globalization;
using System.IO;

using Deveel.Data.Serialization;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlCharacterTypeTests {
		[Theory]
		[InlineData(SqlTypeCode.VarChar, -1, null)]
		[InlineData(SqlTypeCode.VarChar, 255, "en-US")]
		[InlineData(SqlTypeCode.String, -1, "nb-NO")]
		[InlineData(SqlTypeCode.Char, 2, null)]
		public static void CreateStringType(SqlTypeCode typeCode, int maxSize, string locale) {
			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(typeCode, maxSize, culture);

			Assert.Equal(typeCode, type.TypeCode);
			Assert.Equal(maxSize, type.MaxSize);
			Assert.Equal(maxSize > 0, type.HasMaxSize);
			Assert.Equal(locale, type.Locale == null ? null : type.Locale.Name);
			Assert.True(type.IsIndexable);
			Assert.False(type.IsReference);
			Assert.False(type.IsLargeObject);
			Assert.True(type.IsPrimitive);
		}

		[Theory]
		[InlineData(SqlTypeCode.VarChar, -1, null, "VARCHAR")]
		[InlineData(SqlTypeCode.VarChar, 255, "en-US", "VARCHAR(255) COLLATE 'en-US'")]
		[InlineData(SqlTypeCode.String, -1, "nb-NO", "STRING COLLATE 'nb-NO'")]
		[InlineData(SqlTypeCode.Char, 2, null, "CHAR(2)")]
		[InlineData(SqlTypeCode.LongVarChar, -1, null, "LONG CHARACTER VARYING")]
		public static void GetTypeString(SqlTypeCode typeCode, int maxSize, string locale, string expected) {
			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(typeCode, maxSize, culture);

			var sql = type.ToString();
			Assert.Equal(expected, sql);
		}


		[Theory]
		[InlineData("the quick brown fox", "the brown quick fox", 15)]
		[InlineData("ab12334", "kj12345", -10)]
		public static void CompareSimpleStrings(string s1, string s2, int expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var type = PrimitiveTypes.String();
			Assert.True(type.IsComparable(type));

			var result = type.Compare(sqlString1, sqlString2);

			Assert.Equal(expected, result);
		}

		[Theory]
		[InlineData("12345", "12345", null, false)]
		[InlineData("abc", "cde", null, true)]
		[InlineData("aaaaaaaabaaa", "aaaaabaaaa", null, true)]
		[InlineData("Abc", "abc", null, true)]
		[InlineData("ås", "øs", "nb-NO", true)]
		[InlineData("yolo", "yol", null, false)]
		public static void Greater(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool) type.Greater(sqlString1, sqlString2));
		}

		[Theory]
		[InlineData("ereee", "123bd", null, false)]
		[InlineData("abc1234", "abc1234", null, true)]
		public static void GreaterOrEqual(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool)type.GreaterOrEqual(sqlString1, sqlString2));
		}


		[Theory]
		[InlineData("12345", "12345", null, false)]
		[InlineData("abc", "cde", null, false)]
		[InlineData("aaaaaaaabaaa", "aaaaabaaaa", null, false)]
		[InlineData("Abc", "abc", null, false)]
		[InlineData("ås", "øs", "nb-NO", false)]
		[InlineData("yolo", "yol", null, true)]
		public static void Smaller(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool)type.Less(sqlString1, sqlString2));
		}

		[Theory]
		[InlineData("abc", "cde", null, false)]
		public static void SmallerOrEqual(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool)type.LessOrEqual(sqlString1, sqlString2));
		}

		[Theory]
		[InlineData("abc12345", "abc12345", null, true)]
		[InlineData("ab12345", "abc12345",  null, false)]
		[InlineData("the brown\n", "the brown", null, false)]
		public static void Equal(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool)type.Equal(sqlString1, sqlString2));
		}

		[Theory]
		[InlineData("abc12345", "abc12345", null, false)]
		[InlineData("ab12345", "abc12345", null, true)]
		public static void NotEqual(string s1, string s2, string locale, bool expected) {
			var sqlString1 = new SqlString(s1);
			var sqlString2 = new SqlString(s2);

			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(SqlTypeCode.String, -1, culture);

			Assert.Equal(expected, (bool)type.NotEqual(sqlString1, sqlString2));
		}

		[Fact]
		public static void InvalidXOr() {
			InvalidOp(type => type.XOr);
		}

		[Fact]
		public static void InvalidOr() {
			InvalidOp(type => type.Or);
		}

		[Fact]
		public static void InvalidAnd() {
			InvalidOp(sqlType => sqlType.And);
		}

		[Fact]
		public static void InvalidAdd() {
			InvalidOp(sqlType => sqlType.And);
		}

		[Fact]
		public static void InvalidSubtract() {
			InvalidOp(sqlType => sqlType.Subtract);
		}

		[Fact]
		public static void InvalidMultiply() {
			InvalidOp(sqlType => sqlType.Multiply);
		}

		[Fact]
		public static void InvalidDivide() {
			InvalidOp(type => type.Divide);
		}

		[Fact]
		public static void InvalidModulo() {
			InvalidOp(type => type.Modulo);
		}

		[Fact]
		public static void InvalidNegate() {
			InvalidOp(type => type.Negate);
		}

		[Fact]
		public static void InvalidPlus() {
			InvalidOp(type => type.UnaryPlus);
		}

		[Fact]
		public static void InvalidNot() {
			InvalidOp(type => type.Not);
		}

		private static void InvalidOp(Func<SqlType, Func<ISqlValue, ISqlValue, ISqlValue>> selector) {
			var s1 = new SqlString("ab");
			var s2 = new SqlString("cd");

			var type = new SqlCharacterType(SqlTypeCode.String, -1, null);
			var op = selector(type);
			var result = op(s1, s2);
			Assert.NotNull(result);
			Assert.IsType<SqlNull>(result);
		}

		private static void InvalidOp(Func<SqlType, Func<ISqlValue, ISqlValue>> selector) {
			var s1 = new SqlString("foo");

			var type = new SqlCharacterType(SqlTypeCode.String, -1, null);
			var op = selector(type);
			var result = op(s1);
			Assert.NotNull(result);
			Assert.IsType<SqlNull>(result);
		}

		[Theory]
		[InlineData("true", SqlTypeCode.Boolean, 1, 0, true)]
		[InlineData("FALSE", SqlTypeCode.Boolean, 1, 0, false)]
		[InlineData("TRUE", SqlTypeCode.Boolean, 1, 0, true)]
		[InlineData("5628829.000021192", SqlTypeCode.Double, -1, -1, 5628829.000021192)]
		[InlineData("NaN", SqlTypeCode.Double, -1, -1, Double.NaN)]
		[InlineData("-6773.09222222", SqlTypeCode.Double, -1, -1, -6773.09222222)]
		[InlineData("8992e78", SqlTypeCode.Double, -1, -1, 8992e78)]
		[InlineData("677110199911111", SqlTypeCode.BigInt, -1, -1, 677110199911111)]
		[InlineData("215", SqlTypeCode.TinyInt, -1, -1, 215)]
		[InlineData("71182992", SqlTypeCode.Integer, -1, -1, 71182992)]
		[InlineData("the quick brown fox", SqlTypeCode.VarChar, 255, -1, "the quick brown fox")]
		[InlineData("lorem ipsum dolor sit amet", SqlTypeCode.Char, 11, -1, "lorem ipsum")]
		[InlineData("do", SqlTypeCode.Char, 10, -1, "do        ")]
		public static void Cast(string value, SqlTypeCode destTypeCode, int p, int s, object expected) {
			OperatorsUtil.Cast(value, destTypeCode, p, s, expected);
		}

		[Theory]
		[InlineData("2011-01-11", SqlTypeCode.Date, "2011-01-11")]
		[InlineData("2014-01-21T02:10:16.908", SqlTypeCode.Date, "2014-01-21")]
		[InlineData("2014-01-21T02:10:16.908", SqlTypeCode.TimeStamp, "2014-01-21T02:10:16.908")]
		[InlineData("02:10:16.908", SqlTypeCode.Time, "02:10:16.908")]
		[InlineData("2014-01-21T02:10:16.908", SqlTypeCode.Time, "02:10:16.908")]
		public static void CastToDateTime(string s, SqlTypeCode typeCode, string expected) {
			var expectedResult = SqlDateTime.Parse(expected);
			Cast(s, typeCode, -1, -1, expectedResult);
		}

		[Theory]
		[InlineData("2.12:03:20.433", "2.12:03:20.433")]
		public static void CastToDayToSecond(string s, string expected) {
			var expectedResult = SqlDayToSecond.Parse(expected);
			Cast(s, SqlTypeCode.DayToSecond, -1, -1, expectedResult);
		}

		[Theory]
		[InlineData("1.22", 34)]
		[InlineData("15", 15)]
		public static void CastToYearToMonth(string s, int expected) {
			var exp = new SqlYearToMonth(expected);
			Cast(s, SqlTypeCode.YearToMonth, -1, -1, exp);
		}

		[Theory]
		[InlineData(SqlTypeCode.VarChar, -1, null)]
		[InlineData(SqlTypeCode.VarChar, 255, "en-US")]
		[InlineData(SqlTypeCode.String, -1, "nb-NO")]
		[InlineData(SqlTypeCode.Char, 2, null)]
		[InlineData(SqlTypeCode.LongVarChar, -1, null)]
		public static void Serialize(SqlTypeCode typeCode, int maxSize, string locale) {
			var culture = String.IsNullOrEmpty(locale) ? null : new CultureInfo(locale);
			var type = new SqlCharacterType(typeCode, maxSize, culture);

			var result = BinarySerializeUtil.Serialize(type);
			Assert.Equal(type, result);
		}
	}
}