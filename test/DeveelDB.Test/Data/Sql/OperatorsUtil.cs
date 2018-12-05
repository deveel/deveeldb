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

namespace Deveel.Data.Sql {
	public static class OperatorsUtil {
		public static void Binary(SqlType type, Func<SqlType, Func<ISqlValue, ISqlValue, SqlBoolean>> selector, object value1, object value2, bool expected) {
			var x = SqlValueUtil.FromObject(value1);
			var y = SqlValueUtil.FromObject(value2);

			var op = selector(type);
			var result = op(x, y);
			Assert.Equal(expected, (bool)result);
		}

		public static void Binary(SqlType type, Func<SqlType, Func<ISqlValue, ISqlValue, ISqlValue>> selector, object value1, object value2, object expected) {
			var x = SqlValueUtil.FromObject(value1);
			var y = SqlValueUtil.FromObject(value2);
			var exp = SqlValueUtil.FromObject(expected);

			var op = selector(type);
			var result = op(x, y);

			Assert.Equal(exp, result);
		}

		public static void Binary(Func<ISqlValue, ISqlValue, ISqlValue> op, object value1, object value2, object expected) {
			var x = SqlValueUtil.FromObject(value1);
			var y = SqlValueUtil.FromObject(value2);
			var exp = SqlValueUtil.FromObject(expected);

			var result = op(x, y);

			Assert.Equal(exp, result);
		}

		public static void Unary(Func<SqlType, Func<ISqlValue, ISqlValue>> selector, object value,
			object expected) {
			Unary(SqlTypeUtil.FromValue(value), selector, value, expected);
		}

		public static void Unary(SqlType type, Func<SqlType, Func<ISqlValue, ISqlValue>> selector, object value,
			object expected) {
			var x = SqlValueUtil.FromObject(value);
			var exp = SqlValueUtil.FromObject(expected);

			var op = selector(type);
			var result = op(x);

			Assert.Equal(exp, result);
		}

		public static void Cast(SqlType srcType, object value, SqlType destType, object expected) {
			var x = SqlValueUtil.FromObject(value);

			Assert.True(srcType.CanCastTo(x, destType));

			var exp = SqlValueUtil.FromObject(expected);

			var result = srcType.Cast(x, destType);

			Assert.Equal(exp, result);
		}

		public static void Cast(SqlTypeCode srcTypeCode, int p1, int s1, object value, SqlTypeCode destTypeCode, int p2, int s2,
			object expected) {
			var srcType = PrimitiveTypes.Type(srcTypeCode, new {precision = p1, scale = s1, maxSize = p1, size = p1});
			var destType = PrimitiveTypes.Type(destTypeCode, new {precision = p2, scale = s2, maxSize = p2, size = p2});

			Cast(srcType, value, destType, expected);
		}

		public static void Cast(object value, SqlTypeCode destTypeCode, int p, int s, object expected) {
			var srcType = SqlTypeUtil.FromValue(value);
			var destType = PrimitiveTypes.Type(destTypeCode, new { precision = p, scale = s, maxSize = p, size = p });

			Cast(srcType, value, destType, expected);
		}
	}
}