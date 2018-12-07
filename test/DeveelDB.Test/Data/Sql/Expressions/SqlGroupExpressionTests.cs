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
	public static class SqlGroupExpressionTests {
		[Fact]
		public static async Task ReduceGroup() {
			var exp = SqlExpression.Binary(SqlExpressionType.Equal,
				SqlExpression.Constant(SqlObject.Boolean(true)),
				SqlExpression.Constant(SqlObject.Boolean(false)));
			var group = SqlExpression.Group(exp);

			var reduced = await group.ReduceAsync(null);
			Assert.NotNull(reduced);
			Assert.IsType<SqlConstantExpression>(reduced);
			Assert.IsType<SqlObject>(((SqlConstantExpression) reduced).Value);
		}

		[Fact]
		public static void GetGroupString() {
			var exp = SqlExpression.Binary(SqlExpressionType.Equal,
				SqlExpression.Constant(SqlObject.Integer(33)),
				SqlExpression.Constant(SqlObject.Integer(54)));
			var group = SqlExpression.Group(exp);

			const string expected = "(33 = 54)";
			var sql = group.ToString();

			Assert.Equal(expected, sql);
		}
	}
}