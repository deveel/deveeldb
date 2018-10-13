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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Variables {
	public static class VariableTests {
		[Theory]
		[InlineData("a", SqlTypeCode.Double, -1, -1, false, null)]
		[InlineData("b", SqlTypeCode.Boolean, -1, -1, true, true)]
		public static void CreateVariable(string name, SqlTypeCode typeCode, int p, int s, bool constant,
			object defaultValue) {
			var exp = FormDefaultValue(defaultValue);
			var type = PrimitiveTypes.Type(typeCode, new {precision = p, scale = s, maxSize = p});
			var variable = new Variable(name, type, constant, exp);

			Assert.NotNull(variable.VariableInfo);
			Assert.Equal(name, variable.Name);
			Assert.NotNull(variable.Type);
			Assert.Equal(constant, variable.Constant);
			Assert.Equal(type, variable.Type);
			Assert.Equal(defaultValue != null, variable.VariableInfo.HasDefaultValue);
		}

		[Theory]
		[InlineData("a.", SqlTypeCode.Boolean)]
		[InlineData("$b", SqlTypeCode.Double)]
		public static void CreateWithInvalidName(string name, SqlTypeCode typeCode) {
			var type = PrimitiveTypes.Type(typeCode);
			Assert.Throws<ArgumentException>(() => new Variable(name, type));
		}

		[Theory]
		[InlineData("a", SqlTypeCode.Double, -1, -1, false, null, ":a DOUBLE")]
		[InlineData("b", SqlTypeCode.Boolean, -1, -1, true, true, ":b CONSTANT BOOLEAN := TRUE")]
		[InlineData("c", SqlTypeCode.Numeric, 20, 5, false, 3445.021, ":c NUMERIC(20,5) := 3445.021")]
		public static void GetVariableString(string name, SqlTypeCode typeCode, int p, int s, bool constant,
			object defaultValue, string expected) {
			var exp = FormDefaultValue(defaultValue);
			var type = PrimitiveTypes.Type(typeCode, new { precision = p, scale = s, maxSize = p });
			var variable = new Variable(name, type, constant, exp);

			var sql = variable.ToString();
			Assert.Equal(expected, sql);
		}

		private static SqlExpression FormDefaultValue(object value) {
			if (value == null)
				return null;

			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			return SqlExpression.Constant(obj);
		}
	}
}