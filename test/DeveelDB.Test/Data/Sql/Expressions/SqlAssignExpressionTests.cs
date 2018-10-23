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

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlAssignExpressionTests {
		[Theory]
		[InlineData("a.b", true)]
		public static void CreateReferenceAssign(string name, object value) {
			var objName = ObjectName.Parse(name);
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var refAssign = SqlExpression.ReferenceAssign(objName, exp);

			Assert.NotNull(refAssign.ReferenceName);
			Assert.NotNull(refAssign.Value);

			Assert.Equal(objName, refAssign.ReferenceName);
		}

		//[Theory]
		//[InlineData("a.b", true)]
		//public static void SerializeReferenceAssign(string name, object value) {
		//	var objName = ObjectName.Parse(name);
		//	var obj = SqlObject.New(SqlValueUtil.FromObject(value));
		//	var exp = SqlExpression.Constant(obj);

		//	var refAssign = SqlExpression.ReferenceAssign(objName, exp);
		//	var result = BinarySerializeUtil.Serialize(refAssign);

		//	Assert.Equal(objName, result.ReferenceName);
		//	Assert.IsType<SqlConstantExpression>(result.Value);
		//}


		[Theory]
		[InlineData("a.b", true, "a.b = TRUE")]
		public static void GetReferenceAssignString(string name, object value, string expected) {
			var objName = ObjectName.Parse(name);
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var refAssign = SqlExpression.ReferenceAssign(objName, exp);
			var sql = refAssign.ToString();

			Assert.Equal(expected, sql);
		}
	}
}