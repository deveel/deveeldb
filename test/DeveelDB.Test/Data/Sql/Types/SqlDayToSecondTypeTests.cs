using System;
using System.IO;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlDayToSecondTypeTests {
		[Theory]
		[InlineData("22:19:34", "2.00:00:20.444", "2.22:19:54.444")]
		public static void AddDayToSecond(string value1, string value2, string expected) {
			Binary(type => type.Add, value1, value2, expected);
		}

		[Theory]
		[InlineData("22:19:34", "2.00:00:20.444", "-1.01:40:46.444")]
		public static void SubtractDayToSecond(string value1, string value2, string expected) {
			Binary(type => type.Subtract, value1, value2, expected);
		}

		[Theory]
		[InlineData("11:20:05.553", SqlTypeCode.Char, 12, "11:20:05.553")]
		[InlineData("2.15:34:16.524", SqlTypeCode.Char, 20, "2.15:34:16.5240000  ")]
		[InlineData("4.19:11:01.861", SqlTypeCode.VarChar, 200, "4.19:11:01.8610000")]
		public static void CastToString(string value, SqlTypeCode destTypeCode, int size, string expexted) {
			var dts = SqlDayToSecond.Parse(value);

			var type = new SqlDayToSecondType();
			var destType = PrimitiveTypes.Type(destTypeCode, new {size});
			var result = type.Cast(dts, destType);

			var exp = SqlValueUtil.FromObject(expexted);
			Assert.NotNull(result);
			Assert.Equal(exp, result);
		}

		[Theory]
		[InlineData("17:09:45.223", SqlTypeCode.VarBinary, 15)]
		public static void CastToBinary(string value, SqlTypeCode destTypeCode, int size) {
			var dts = SqlDayToSecond.Parse(value);

			var type = new SqlDayToSecondType();
			var destType = PrimitiveTypes.Type(destTypeCode, new { size });
			var result = type.Cast(dts, destType);

			Assert.IsType<SqlBinary>(result);

			var binary = (SqlBinary) result;
			var bytes = binary.ToByteArray();

			var back = new SqlDayToSecond(bytes);
			Assert.Equal(dts, back);
		}

		private static void Binary(Func<SqlType, Func<ISqlValue, ISqlValue, ISqlValue>> selector,
			string value1,
			string value2,
			string expected) {
			var type = new SqlDayToSecondType();

			var a = SqlDayToSecond.Parse(value1);
			var b = SqlDayToSecond.Parse(value2);

			var op = selector(type);
			var result = op(a, b);

			var exp = SqlDayToSecond.Parse(expected);

			Assert.Equal(exp, result);
		}

		//TODO:
     //   [Theory]
     //   [InlineData("INTERVAL DAY TO SECOND", SqlTypeCode.DayToSecond)]
	    //public static void ParseString(string s, SqlTypeCode typeCode) {
     //       var type = SqlType.Parse(s);

     //       Assert.NotNull(type);
     //       Assert.Equal(typeCode, type.TypeCode);

     //       Assert.IsType<SqlDayToSecondType>(type);
     //   }
	}
}