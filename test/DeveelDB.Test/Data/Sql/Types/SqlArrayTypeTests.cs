using System;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlArrayTypeTests {
		[Theory]
		[InlineData(4005)]
		[InlineData(92)]
		public static void ConstructArray(int length) {
			var type = new SqlArrayType(length);
			Assert.NotNull(type);
			Assert.Equal(length, type.Length);
		}

		[Theory]
		[InlineData(105, "ARRAY(105)")]
		public static void FormatString(int length, string expected) {
			var type = new SqlArrayType(length);
			Assert.NotNull(type);
			Assert.Equal(length, type.Length);
			Assert.Equal(expected, type.ToString());
		}

		[Theory]
		[InlineData(495, 67, false)]
		[InlineData(123, 123, true)]
		public static void CheckEquality(int length1, int length2, bool expected) {
			var type1 = new SqlArrayType(length1);
			var type2 = new SqlArrayType(length2);

			Assert.Equal(expected, type1.Equals(type2));
		}
	}
}