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
	public static class SqlConditionExpressionTests {
		[Theory]
		[InlineData(true, 223.21, 11, "CASE WHEN TRUE THEN 223.21 ELSE 11 END")]
		public static void GetConditionString(bool test, object ifTrue, object ifFalse, string expected) {
			var testExp = SqlExpression.Constant(SqlObject.Boolean(test));
			var ifTrueExp = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(ifTrue)));
			var ifFalseExp = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(ifFalse)));

			var condition = SqlExpression.Condition(testExp, ifTrueExp, ifFalseExp);

			var sql = condition.ToString();
			Assert.Equal(expected, sql);
		}


		[Theory]
		[InlineData(true, "I am", "You are", "I am")]
		[InlineData(false, "I am", "You are", "You are")]
		public static async Task ReduceCondition(bool test, object ifTrue, object ifFalse, object expected) {
			var testExp = SqlExpression.Constant(SqlObject.Boolean(test));
			var ifTrueExp = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(ifTrue)));
			var ifFalseExp = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(ifFalse)));

			var condition = SqlExpression.Condition(testExp, ifTrueExp, ifFalseExp);

			var result = await condition.ReduceAsync(null);

			Assert.IsType<SqlConstantExpression>(result);

			var expectedValue = SqlObject.New(SqlValueUtil.FromObject(expected));
			Assert.Equal(expectedValue, ((SqlConstantExpression)result).Value);
		}
	}
}