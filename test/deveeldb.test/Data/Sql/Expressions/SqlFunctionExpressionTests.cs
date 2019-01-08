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
using Deveel.Data.Sql.Methods;
using Deveel.Data.Sql.Types;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlFunctionExpressionTests : IDisposable {
		private IContext context;

		public SqlFunctionExpressionTests() {
			var methodInfo = new SqlFunctionInfo(ObjectName.Parse("sys.func1"), PrimitiveTypes.Integer());
			methodInfo.Parameters.Add(new SqlParameterInfo("a", PrimitiveTypes.VarChar(155)));
			methodInfo.Parameters.Add(new SqlParameterInfo("b", PrimitiveTypes.Integer(),
				SqlExpression.Constant(SqlObject.Null)));
			var method = new SqlFunctionDelegate(methodInfo,
				ctx => { return Task.FromResult(ctx.Value("a").Add(ctx.Value("b"))); });

			var resolver = new Mock<IMethodResolver>();
			resolver.Setup(x => x.ResolveMethod(It.IsAny<IContext>(), It.IsAny<Invoke>()))
				.Returns<IContext, Invoke>((context, invoke) => method);

			var services = new ServiceContainer();
			services.RegisterInstance(resolver.Object);

			var mock = new Mock<IContext>();
			mock.SetupGet(x => x.Scope)
				.Returns(services);

			context = mock.Object;
		}

		[Fact]
		public void CreateNew() {
			var exp = SqlExpression.Function(ObjectName.Parse("sys.func2"),
				new[] {new InvokeArgument(SqlExpression.Constant(SqlObject.Bit(false)))});

			Assert.Equal(SqlExpressionType.Function, exp.ExpressionType);
			Assert.NotNull(exp.Arguments);
			Assert.NotEmpty(exp.Arguments);
			Assert.Single(exp.Arguments);
		}

		[Fact]
		public void GetSqlType() {
			var function = SqlExpression.Function(ObjectName.Parse("sys.Func1"),
				new InvokeArgument("a", SqlObject.String(new SqlString("foo"))));

			Assert.True(function.IsReference);
			var type = function.GetSqlType(context);

			Assert.Equal(PrimitiveTypes.Integer(), type);
		}

		[Fact]
		public async Task ReduceFromExisting() {
			var function = SqlExpression.Function(ObjectName.Parse("sys.Func1"),
				new InvokeArgument("a", SqlObject.String(new SqlString("foo"))));
			Assert.True(function.CanReduce);
			var result = await function.ReduceAsync(context);

			Assert.NotNull(result);
			Assert.IsType<SqlConstantExpression>(result);

			var value = ((SqlConstantExpression) result).Value;
			Assert.Equal(SqlObject.Null, value);
		}

		[Fact]
		public static void GetNamedString() {
			var function = SqlExpression.Function(ObjectName.Parse("sys.Func1"),
				new InvokeArgument("a", SqlObject.BigInt(33)));

			const string sql = "sys.Func1(a => 33)";
			Assert.Equal(sql, function.ToString());
		}

		[Fact]
		public static void GetAnonString() {
			var function =
				SqlExpression.Function(ObjectName.Parse("sys.Func1"), new InvokeArgument(SqlObject.BigInt(33)));

			const string sql = "sys.Func1(33)";
			Assert.Equal(sql, function.ToString());
		}


		public void Dispose() {
			context?.Dispose();
		}
	}
}