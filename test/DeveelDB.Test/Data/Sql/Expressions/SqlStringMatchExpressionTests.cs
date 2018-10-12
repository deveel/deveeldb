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
	public static class SqlStringMatchExpressionTests {
		[Theory]
		[InlineData(SqlExpressionType.Like, "antonello", "anto%")]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the brown%")]
		public static void CreateStringMatch(SqlExpressionType expressionType, string value, string pattern) {
			var exp = SqlExpression.StringMatch(expressionType,
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(pattern))), null);

			Assert.NotNull(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.NotNull(exp.Left);
			Assert.NotNull(exp.Pattern);
		}

		[Theory]
		[InlineData(SqlExpressionType.Like, "antonello", "anto%", true)]
		[InlineData(SqlExpressionType.Like, "antonello", "apto%", false)]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the brown%", true)]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the quick%", false)]
		public static async Task ReduceStringMatch(SqlExpressionType expressionType, string value, string pattern,
			bool expected) {
			var exp = SqlExpression.StringMatch(expressionType,
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(pattern))), null);

			Assert.NotNull(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.NotNull(exp.Left);
			Assert.NotNull(exp.Pattern);

			var reduced = await exp.ReduceAsync(null);

			Assert.NotNull(reduced);
			Assert.IsType<SqlConstantExpression>(reduced);

			var result = ((SqlConstantExpression) reduced).Value;
			Assert.IsType<SqlBoolean>(result.Value);
			Assert.Equal(expected, (bool) ((SqlBoolean) result.Value));
		}

		[Theory]
		[InlineData(SqlExpressionType.Like, "antonello", "anto%", "\\", true)]
		[InlineData(SqlExpressionType.Like, "antonello", "apto%", "|", false)]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the brown%", "\\", true)]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the quick%", "\\", false)]
		public static async Task ReduceStringMatchWithEscape(SqlExpressionType expressionType, string value, string pattern,
			string escape, bool expected) {
			var exp = SqlExpression.StringMatch(expressionType,
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(pattern))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(escape))));

			Assert.NotNull(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.NotNull(exp.Left);
			Assert.NotNull(exp.Pattern);

			var reduced = await exp.ReduceAsync(null);

			Assert.NotNull(reduced);
			Assert.IsType<SqlConstantExpression>(reduced);

			var result = ((SqlConstantExpression) reduced).Value;
			Assert.IsType<SqlBoolean>(result.Value);
			Assert.Equal(expected, (bool) ((SqlBoolean) result.Value));
		}


		[Theory]
		[InlineData(SqlExpressionType.Like, "antonello", "anto%", "'antonello' LIKE 'anto%'")]
		[InlineData(SqlExpressionType.NotLike, "the quick brown fox", "the quick%",
			"'the quick brown fox' NOT LIKE 'the quick%'")]
		public static void GetStringMatchString(SqlExpressionType expressionType, string value, string pattern,
			string expected) {
			var exp = SqlExpression.StringMatch(expressionType,
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(pattern))), null);

			var sql = exp.ToString();
			Assert.Equal(expected, sql);
		}


		[Theory]
		[InlineData(SqlExpressionType.Like, "antonello", "anto%", "\\", "'antonello' LIKE 'anto%' ESCAPE '\\'")]
		public static void GetStringMatchStringWithEscape(SqlExpressionType expressionType, string value, string pattern,
			string escape, string expected) {
			var exp = SqlExpression.StringMatch(expressionType,
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(pattern))),
				SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(escape))));

			var sql = exp.ToString();
			Assert.Equal(expected, sql);
		}

		//TODO:
		//[Theory]
		//[InlineData("a LIKE 'anto%'", SqlExpressionType.Like)]
		//[InlineData("a NOT LIKE '%hell%'", SqlExpressionType.NotLike)]
		//public static void ParseString(string s, SqlExpressionType expressionType) {
		//	var exp = SqlExpression.Parse(s);

		//	Assert.NotNull(exp);
		//	Assert.IsType<SqlStringMatchExpression>(exp);
		//	Assert.Equal(expressionType, exp.ExpressionType);
		//}
	}
}