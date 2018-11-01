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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Methods {
	public class SqlFunctionTests {
		private QueryContext context;

		public SqlFunctionTests() {
			var mock = new Mock<IContext>();
			mock.SetupGet(x => x.Scope)
				.Returns(new ServiceContainer());
			mock.SetupGet(x => x.ContextName)
				.Returns("test");

			context = new QueryContext(mock.Object, null, null);
		}

		[Fact]
		public static void MakeFunctionInfo() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));

			Assert.Equal(name, info.MethodName);
			Assert.NotNull(info.ReturnType);
			Assert.Equal(SqlTypeCode.Integer, info.ReturnType.TypeCode);
		}

		[Fact]
		public static void GetString() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));
			var function = new SqlFunctionDelegate(info, ctx => {
				var a = ctx.Value("a");
				return Task.FromResult(a.Multiply(SqlObject.BigInt(2)));
			});

			var sql = $"FUNCTION a.func(a INTEGER) RETURNS INTEGER";
			Assert.Equal(sql, function.ToString());
		}

		[Fact]
		public static void MatchInvoke() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));

			var function = new SqlFunctionDelegate(info, context => Task.CompletedTask);

			var invoke = new Invoke(name, new []{new InvokeArgument(SqlObject.BigInt(11)) });

			Assert.True(function.Matches(null, invoke));
		}

		[Fact]
		public async Task ExecuteWithSequentialArgs() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));
			var function = new SqlFunctionDelegate(info, ctx => {
				var a = ctx.Value("a");
				return Task.FromResult(a.Multiply(SqlObject.BigInt(2)));
			});

			Assert.Equal(name, info.MethodName);
			Assert.Equal(FunctionType.Scalar, function.FunctionType);
			Assert.NotNull(info.ReturnType);
			Assert.Equal(SqlTypeCode.Integer, info.ReturnType.TypeCode);

			var result = await function.ExecuteAsync(context, SqlObject.Integer(22));

			Assert.NotNull(result);
			Assert.True(result.HasReturnedValue);
			Assert.NotNull(result.ReturnedValue);
			Assert.IsType<SqlConstantExpression>(result.ReturnedValue);
		}

		[Fact]
		public async Task ExecuteWithNamedArgs() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));
			var function = new SqlFunctionDelegate(info, ctx => {
				var a = ctx.Value("a");
				return Task.FromResult(a.Multiply(SqlObject.BigInt(2)));
			});

			Assert.Equal(name, info.MethodName);
			Assert.Equal(FunctionType.Scalar, function.FunctionType);
			Assert.NotNull(info.ReturnType);
			Assert.Equal(SqlTypeCode.Integer, info.ReturnType.TypeCode);

			var result = await function.ExecuteAsync(context, new InvokeArgument("a", SqlObject.Integer(22)));

			Assert.NotNull(result);
			Assert.True(result.HasReturnedValue);
			Assert.NotNull(result.ReturnedValue);
			Assert.IsType<SqlConstantExpression>(result.ReturnedValue);
		}

		[Fact]
		public async Task ExecuteWithNamedArgsAndDefaultValue() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, PrimitiveTypes.Integer());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));
			info.Parameters.Add(new SqlParameterInfo("b",
				PrimitiveTypes.String(),
				SqlExpression.Constant(SqlObject.String(new SqlString("test")))));

			var function = new SqlFunctionDelegate(info, ctx => {
				var a = ctx.Value("a");
				var b = ctx.Value("b");
				Assert.NotNull(b);
				return Task.FromResult(a.Multiply(SqlObject.BigInt(2)));
			});

			Assert.Equal(name, info.MethodName);
			Assert.Equal(FunctionType.Scalar, function.FunctionType);
			Assert.NotNull(info.ReturnType);
			Assert.Equal(SqlTypeCode.Integer, info.ReturnType.TypeCode);

			var result = await function.ExecuteAsync(context, new InvokeArgument("a", SqlObject.Integer(22)));

			Assert.NotNull(result);
			Assert.True(result.HasReturnedValue);
			Assert.NotNull(result.ReturnedValue);
			Assert.IsType<SqlConstantExpression>(result.ReturnedValue);
		}

		[Fact]
		public void ResolveDeterministricReturnType() {
			var name = ObjectName.Parse("a.func");
			var info = new SqlFunctionInfo(name, new SqlDeterministicType());
			info.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.Integer()));
			info.Parameters.Add(new SqlParameterInfo("b",
				PrimitiveTypes.String(),
				SqlExpression.Constant(SqlObject.String(new SqlString("test")))));

			var function = new SqlFunctionDelegate(info, ctx => {
				var a = ctx.Value("a");
				var b = ctx.Value("b");
				Assert.NotNull(b);
				return Task.FromResult(a.Multiply(SqlObject.BigInt(2)));
			});

			var returnType = function.ReturnType(context,
				new Invoke(name, new[] {new InvokeArgument(SqlObject.Integer(33)), new InvokeArgument(SqlObject.Integer(2))}));

			Assert.Equal(PrimitiveTypes.Integer(), returnType);
		}
	}
}