using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlArrayTests {
		[Theory]
		[InlineData(40)]
		public static void ConstructArray(int length) {
			var type = new SqlArrayType(length);

			var array = new SqlArray(new SqlExpression[length]);
			Assert.Equal(length, array.Length);

			Assert.True(type.IsInstanceOf(array));
		}
	}
}