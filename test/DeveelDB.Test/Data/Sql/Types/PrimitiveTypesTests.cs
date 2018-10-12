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
using System.Collections.Generic;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class PrimitiveTypesTests {
		[Theory]
		[InlineData("varchar", true)]
		[InlineData("int", true)]
		[InlineData("day to second", true)]
		[InlineData("object", false)]
		[InlineData("TINYINT", true)]
		[InlineData("USERObject", false)]
		[InlineData("DATE", true)]
		[InlineData("YEAR TO MONTH", true)]
		public static void IsStringPrimitive(string name, bool expected) {
			Assert.Equal(expected, PrimitiveTypes.IsPrimitive(name));
		}

		[Theory]
		[InlineData(SqlTypeCode.Type, false)]
		[InlineData(SqlTypeCode.Boolean, true)]
		[InlineData(SqlTypeCode.Blob, true)]
		[InlineData(SqlTypeCode.BigInt, true)]
		[InlineData(SqlTypeCode.VarNumeric, true)]
		public static void IsCodePrimitive(SqlTypeCode typeCode, bool expected) {
			Assert.Equal(expected, PrimitiveTypes.IsPrimitive(typeCode));
		}

		[Theory]
		[InlineData(SqlTypeCode.Bit, null, null, null, null)]
		[InlineData(SqlTypeCode.Boolean, null, null, null, null)]
		[InlineData(SqlTypeCode.BigInt, null, null, null, null)]
		[InlineData(SqlTypeCode.Integer, null, null, null, null)]
		[InlineData(SqlTypeCode.TinyInt, null, null, null, null)]
		[InlineData(SqlTypeCode.SmallInt, null, null, null, null)]
		[InlineData(SqlTypeCode.Double, null, null, null, null)]
		[InlineData(SqlTypeCode.Float, null, null, null, null)]
		[InlineData(SqlTypeCode.Decimal, "Precision", 22, "Scale", 2)]
		[InlineData(SqlTypeCode.Decimal, "Precision", 10, "Scale", 2)]
		[InlineData(SqlTypeCode.Numeric, "precision", 10, "scale", 4)]
		[InlineData(SqlTypeCode.VarNumeric, null, null, null, null)]
		[InlineData(SqlTypeCode.Char, null, null, null, null)]
		[InlineData(SqlTypeCode.Char, "Size", 200, null, null)]
		[InlineData(SqlTypeCode.VarChar, "maxSize", 255, null, null)]
		[InlineData(SqlTypeCode.VarChar, null, null, null, null)]
		[InlineData(SqlTypeCode.Clob, "size", 40677, null, null)]
		[InlineData(SqlTypeCode.Date, null, null, null, null)]
		[InlineData(SqlTypeCode.Time, null, null, null, null)]
		[InlineData(SqlTypeCode.TimeStamp, null, null, null, null)]
		[InlineData(SqlTypeCode.Binary, "size", 1024, null, null)]
		[InlineData(SqlTypeCode.Binary, null, null, null, null)]
		[InlineData(SqlTypeCode.VarBinary, null, null, null, null)]
		[InlineData(SqlTypeCode.YearToMonth, null, null, null, null)]
		[InlineData(SqlTypeCode.DayToSecond, null, null, null, null)]
		public static void ResolveType(SqlTypeCode typeCode, string propName1, object prop1, string propName2, object prop2)
		{
			var props = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
			if (!String.IsNullOrEmpty(propName1))
				props.Add(propName1, prop1);
			if (!String.IsNullOrEmpty(propName2))
				props.Add(propName2, prop2);

			var sqlType = PrimitiveTypes.Type(typeCode, props);
			Assert.NotNull(sqlType);
			Assert.Equal(typeCode, sqlType.TypeCode);
			Assert.True(sqlType.IsPrimitive);
			Assert.False(sqlType.IsReference);
		}

		[Theory]
		[InlineData(SqlTypeCode.Binary)]
		[InlineData(SqlTypeCode.Array)]
		public static void GetInvalidNumeric(SqlTypeCode typeCode)
		{
			Assert.Throws<ArgumentException>(() => PrimitiveTypes.Numeric(typeCode, 20, 10));
		}

		[Fact]
		public static void GetInvalidBoolean()
		{
			Assert.Throws<ArgumentException>(() => PrimitiveTypes.Boolean(SqlTypeCode.Blob));
		}
	}
}