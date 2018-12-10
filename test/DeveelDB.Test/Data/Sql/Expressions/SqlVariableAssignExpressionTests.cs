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

using Deveel.Data.Query;
using Deveel.Data.Services;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlVariableAssignExpressionTests {
		private IContext context;

		public SqlVariableAssignExpressionTests() {
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

			context = mock.Object;
		}

		[Theory]
		[InlineData("b", "test", SqlTypeCode.VarChar, 50)]
		public void GetSqlTypeOfVarAssign(string name, object value, SqlTypeCode typeCode, int p) {
			var exp = SqlExpression.VariableAssign(name, SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value))));

			var varType = exp.GetSqlType(context);
			var type = PrimitiveTypes.Type(typeCode, new {precision = p, maxSize = p, size = p});

			Assert.Equal(type, varType);
		}

		[Theory]
		[InlineData("a", true)]
		public async Task ReduceVarAssign(string name, object value) {
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var varRef = SqlExpression.VariableAssign(name, exp);

			Assert.True(varRef.CanReduce);

			var result = await varRef.ReduceAsync(context);

			Assert.NotNull(result);
			Assert.IsType<SqlConstantExpression>(result);
		}

		[Theory]
		[InlineData("a", true, ":a := TRUE")]
		public void GetVarAssingString(string name, object value, string expected) {
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var varRef = SqlExpression.VariableAssign(name, exp);
			var sql = varRef.ToString();

			Assert.Equal(expected, sql);
		}

		[Theory]
		[InlineData("a", true)]
		public void CreateVarAssign(string name, object value) {
			var obj = SqlObject.New(SqlValueUtil.FromObject(value));
			var exp = SqlExpression.Constant(obj);

			var varRef = SqlExpression.VariableAssign(name, exp);
			Assert.NotNull(varRef.Value);
			Assert.Equal(exp, varRef.Value);
			Assert.Equal(name, varRef.VariableName);
		}
	}
}