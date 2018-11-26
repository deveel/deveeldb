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
using System.Threading.Tasks;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlConstantExpressionTests {
		[Theory]
		[InlineData(65775.499)]
		[InlineData("The quick brown fox")]
		public static void CreateConstant(object value) {
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			Assert.NotNull(exp.Value);
			Assert.Equal(obj, exp.Value);
		}

		[Theory]
		[InlineData(345)]
		public static async void ReduceConstant(object value) {
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			Assert.False(exp.CanReduce);
			Assert.NotNull(exp.Value);

			var reduced = await exp.ReduceAsync(null);

			Assert.IsType<SqlConstantExpression>(reduced);
			Assert.Equal(obj, ((SqlConstantExpression) reduced).Value);
		}

		[Theory]
		[InlineData("TRUE")]
		[InlineData("FALSE")]
		[InlineData("UNKNOWN")]
		[InlineData("NULL")]
		[InlineData("'test string'")]
		[InlineData("7859403.112")]
		public static void ParseString(string s) {
			var exp = SqlExpression.Parse(s);

			Assert.NotNull(exp);
			Assert.IsType<SqlConstantExpression>(exp);

			var constantExp = (SqlConstantExpression) exp;
			Assert.NotNull(constantExp.Value);
			Assert.NotNull(constantExp.Value.Type);
		}
	}
}
