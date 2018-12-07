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

using Deveel.Data.Sql.Parsing;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public class SqlTypeTests : IDisposable {
		private IContext context;

		public SqlTypeTests() {
			context = ContextUtil.NewParseContext();
		}

		[Theory]
		[InlineData("VARBINARY", SqlTypeCode.VarBinary, -1)]
		[InlineData("VARBINARY(1024)", SqlTypeCode.VarBinary, 1024)]
		[InlineData("BINARY(2048)", SqlTypeCode.Binary, 2048)]
		[InlineData("BLOB", SqlTypeCode.Blob, -1)]
		[InlineData("BLOB(45644)", SqlTypeCode.Blob, 45644)]
		[InlineData("BINARY(MAX)", SqlTypeCode.Binary, SqlBinaryType.DefaultMaxSize)]
		[InlineData("LONG BINARY VARYING", SqlTypeCode.LongVarBinary, -1)]
		public void ParseBinaryTypeString(string sql, SqlTypeCode typeCode, int size)
		{
			var type = SqlType.Parse(context, sql);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.IsType<SqlBinaryType>(type);

			var binType = (SqlBinaryType)type;

			Assert.Equal(size, binType.MaxSize);
		}

		[Theory]
		[InlineData("BOOLEAN", SqlTypeCode.Boolean)]
		[InlineData("BIT", SqlTypeCode.Bit)]
		public void ParseBooleanTypeString(string s, SqlTypeCode typeCode) {
			var type = SqlType.Parse(context, s);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.IsType<SqlBooleanType>(type);
		}

		[Theory]
		[InlineData("STRING", SqlTypeCode.String, -1)]
		[InlineData("STRING(200)", SqlTypeCode.String, 200)]
		[InlineData("VARCHAR", SqlTypeCode.VarChar, -1)]
		[InlineData("VARCHAR(233)", SqlTypeCode.VarChar, 233)]
		[InlineData("CHAR(11)", SqlTypeCode.Char, 11)]
		[InlineData("LONG CHARACTER VARYING", SqlTypeCode.LongVarChar, -1)]
		[InlineData("CLOB(30221)", SqlTypeCode.Clob, 30221)]
		[InlineData("VARCHAR(MAX)", SqlTypeCode.VarChar, SqlCharacterType.DefaultMaxSize)]
		public void ParseCharacterTypeString(string sql, SqlTypeCode typeCode, int size) {
			var type = SqlType.Parse(context, sql);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.IsType<SqlCharacterType>(type);

			var charType = (SqlCharacterType) type;
			Assert.Equal(size, charType.MaxSize);
		}

		[Theory]
		[InlineData("TIME", SqlTypeCode.Time)]
		[InlineData("TIMESTAMP", SqlTypeCode.TimeStamp)]
		[InlineData("DATE", SqlTypeCode.Date)]
		[InlineData("DATETIME", SqlTypeCode.DateTime)]
		public void ParseDateTimeTypeString(string s, SqlTypeCode typeCode) {
			var type = SqlType.Parse(context, s);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.IsType<SqlDateTimeType>(type);
		}

		[Theory]
		[InlineData("INTERVAL DAY TO SECOND", SqlTypeCode.DayToSecond)]
		public void ParseDayToSecondTypeString(string s, SqlTypeCode typeCode) {
			var type = SqlType.Parse(context, s);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);

			Assert.IsType<SqlDayToSecondType>(type);
		}

		[Theory]
		[InlineData("INTERVAL YEAR TO MONTH", SqlTypeCode.YearToMonth)]
		public void ParseYearToMonthTypeSring(string s, SqlTypeCode typeCode) {
			var type = SqlType.Parse(context, s);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);

			Assert.IsType<SqlYearToMonthType>(type);
		}

		[Theory]
		[InlineData("INT", SqlTypeCode.Integer, 10, 0)]
		[InlineData("INTEGER", SqlTypeCode.Integer, 10, 0)]
		[InlineData("BIGINT", SqlTypeCode.BigInt, 19, 0)]
		[InlineData("SMALLINT", SqlTypeCode.SmallInt, 5, 0)]
		[InlineData("TINYINT", SqlTypeCode.TinyInt, 3, 0)]
		[InlineData("DOUBLE", SqlTypeCode.Double, 16, -1)]
		[InlineData("FLOAT", SqlTypeCode.Float, 8, -1)]
		[InlineData("REAL", SqlTypeCode.Float, 8, -1)]
		[InlineData("DECIMAL", SqlTypeCode.Decimal, 24, -1)]
		[InlineData("NUMERIC(22, 13)", SqlTypeCode.Numeric, 22, 13)]
		public void ParseNumericTypeString(string sql, SqlTypeCode typeCode, int precision, int scale) {
			var type = SqlType.Parse(context, sql);

			Assert.NotNull(type);
			Assert.Equal(typeCode, type.TypeCode);
			Assert.IsType<SqlNumericType>(type);

			var numericType = (SqlNumericType) type;

			Assert.Equal(precision, numericType.Precision);
			Assert.Equal(scale, numericType.Scale);
		}

		public void Dispose() {
			context?.Dispose();
		}
	}
}