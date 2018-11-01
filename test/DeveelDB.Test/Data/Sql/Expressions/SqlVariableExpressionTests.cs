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

using Deveel.Data.Services;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlVariableExpressionTests : IDisposable {
		private QueryContext context;

		public SqlVariableExpressionTests() {
			var scope = new ServiceContainer();

			var manager = new VariableManager();
			manager.CreateVariable(new VariableInfo("a", PrimitiveTypes.Boolean(), false, SqlExpression.Constant(SqlObject.Boolean(false))));
			manager.CreateVariable(new VariableInfo("b", PrimitiveTypes.VarChar(150), false, null));

			var mock = new Mock<IContext>();
			mock.SetupGet(x => x.Scope)
				.Returns(scope);
			mock.As<IVariableScope>()
				.SetupGet(x => x.Variables)
				.Returns(manager);

			context = new QueryContext(mock.Object, null, null);
		}

		[Theory]
		[InlineData("a")]
		public void CreateVarRef(string name) {
			var varRef = SqlExpression.Variable(name);

			Assert.NotEmpty(varRef.VariableName);
			Assert.Equal(name, varRef.VariableName);
		}

		[Theory]
		[InlineData("a", ":a")]
		[InlineData("ab_test", ":ab_test")]
		public void GetVariableString(string name, string expected) {
			var varRef = SqlExpression.Variable(name);
			var sql = varRef.ToString();

			Assert.Equal(expected, sql);
		}

		[Theory]
		[InlineData("a")]
		public async Task ReduceVariable(string name) {
			var varRef = SqlExpression.Variable(name);

			Assert.True(varRef.CanReduce);
			var result = await varRef.ReduceAsync(context);

			Assert.NotNull(result);
			Assert.IsType<SqlConstantExpression>(result);

			var value = ((SqlConstantExpression)result).Value;
			Assert.NotEqual(SqlObject.Unknown, value);
		}

		[Theory]
		[InlineData("c")]
		public async Task ReduceNotFoundVariable(string name) {
			var varRef = SqlExpression.Variable(name);

			Assert.True(varRef.CanReduce);
			var result = await varRef.ReduceAsync(context);

			Assert.NotNull(result);
			Assert.IsType<SqlConstantExpression>(result);

			var value = ((SqlConstantExpression) result).Value;
			Assert.Equal(SqlObject.Unknown, value);
		}

		[Theory]
		[InlineData("a", SqlTypeCode.Boolean, 50)]
		public void GetSqlType(string name, SqlTypeCode typeCode, int p) {
			var exp = SqlExpression.Variable(name);

			var varType = exp.GetSqlType(context);
			var type = PrimitiveTypes.Type(typeCode, new { precision = p, maxSize = p, size = p });

			Assert.Equal(type, varType);
		}

		// TODO:
		//[Theory]
		//[InlineData(":a", "a")]
		//[InlineData(":1", "1")]
		//public static void ParseString(string s, string varName) {
		//	var exp = SqlExpression.Parse(s);

		//	Assert.NotNull(exp);
		//	Assert.IsType<SqlVariableExpression>(exp);

		//	var varExp = (SqlVariableExpression) exp;
		//	Assert.NotNull(varExp.VariableName);
		//	Assert.Equal(varName, varExp.VariableName);
		//}

		public void Dispose() {
			if (context != null)
				context.Dispose();
		}
	}
}