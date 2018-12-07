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

using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlCastExpressionTests {
		[Theory]
		[InlineData(5634.99, SqlTypeCode.Double, -1, -1)]
		public static void NewCast(object value, SqlTypeCode destTypeCode, int p, int s) {
			var targetType = PrimitiveTypes.Type(destTypeCode, new {precision = p, scale = s, maxSize = p, size = p});
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var cast = SqlExpression.Cast(exp, targetType);

			Assert.NotNull(cast.Value);
			Assert.NotNull(cast.TargetType);
		}

		[Theory]
		[InlineData(5634.99, SqlTypeCode.Double, -1, -1, (double) 5634.99)]
		[InlineData("true", SqlTypeCode.Boolean, -1, -1, true)]
		public static async void ReduceCast(object value, SqlTypeCode destTypeCode, int p, int s, object expected) {
			var targetType = PrimitiveTypes.Type(destTypeCode, new {precision = p, scale = s, maxSize = p, size = p});
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var cast = SqlExpression.Cast(exp, targetType);

			Assert.True(cast.CanReduce);

			var reduced = await cast.ReduceAsync(null);
			Assert.NotNull(reduced);
			Assert.IsType<SqlConstantExpression>(reduced);

			var result = ((SqlConstantExpression) reduced).Value;

			Assert.NotNull(result);
			Assert.Equal(destTypeCode, result.Type.TypeCode);

			var expectedResult = SqlObject.New(SqlValueUtil.FromObject(expected));
			Assert.Equal(expectedResult, result);
		}

		[Theory]
		[InlineData(5634.99, SqlTypeCode.Double, -1, -1)]
		[InlineData("TRUE", SqlTypeCode.Boolean, -1, -1)]
		public static void GetCastType(object value, SqlTypeCode destTypeCode, int p, int s) {
			var targetType = PrimitiveTypes.Type(destTypeCode, new {precision = p, scale = s, maxSize = p, size = p});
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var cast = SqlExpression.Cast(exp, targetType);

			Assert.Equal(targetType, cast.TargetType);
		}

		[Theory]
		[InlineData(5634.99, SqlTypeCode.Double, -1, -1, "CAST(5634.99 AS DOUBLE)")]
		[InlineData("TRUE", SqlTypeCode.Boolean, -1, -1, "CAST('TRUE' AS BOOLEAN)")]
		public static void GetCastString(object value, SqlTypeCode destTypeCode, int p, int s, string expected) {
			var targetType = PrimitiveTypes.Type(destTypeCode, new {precision = p, scale = s, maxSize = p, size = p});
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var cast = SqlExpression.Cast(exp, targetType);
			var sql = cast.ToString();

			Assert.Equal(expected, sql);
		}
	}
}