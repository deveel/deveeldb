using System;

using Deveel.Data.Sql.Expressions;

using Xunit;

namespace Deveel.Data.Sql.Methods {
	public static class InvokeTests {
		[Theory]
		[InlineData("a", 22.03, "v")]
		[InlineData("a.b", 334.21, 22)]
		public static void CreateInvokeWithAnonArguments(string methodName, object value1, object value2) {
			var name = ObjectName.Parse(methodName);
			var arg1 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var arg2 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value2)));

			var invoke = new Invoke(name);
			invoke.Arguments.Add(new InvokeArgument(arg1));
			invoke.Arguments.Add(new InvokeArgument(arg2));

			Assert.Equal(name, invoke.MethodName);
			Assert.Equal(2, invoke.Arguments.Count);
			Assert.False(invoke.IsNamed);
			Assert.False(invoke.Arguments[0].IsNamed);
			Assert.False(invoke.Arguments[1].IsNamed);
		}

		[Theory]
		[InlineData("func1", "a", 22.03, "b", "v")]
		[InlineData("app.func2", "x", 334.21, "y", 22)]
		public static void CreateInvokeWithNamedArguments(string methodName, string argName1, object value1, string argName2, object value2) {
			var name = ObjectName.Parse(methodName);
			var arg1 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var arg2 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value2)));

			var invoke = new Invoke(name);
			invoke.Arguments.Add(new InvokeArgument(argName1, arg1));
			invoke.Arguments.Add(new InvokeArgument(argName2, arg2));

			Assert.Equal(name, invoke.MethodName);
			Assert.Equal(2, invoke.Arguments.Count);
			Assert.True(invoke.IsNamed);
			Assert.True(invoke.Arguments[0].IsNamed);
			Assert.True(invoke.Arguments[1].IsNamed);
		}

		[Theory]
		[InlineData("func1", "a", 22.03, "v")]
		[InlineData("app.func2", "x", 334.21, 22)]
		public static void CreateInvokeWithMixedArguments(string methodName, string argName1, object value1, object value2) {
			var name = ObjectName.Parse(methodName);
			var arg1 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var arg2 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value2)));

			var invoke = new Invoke(name);
			invoke.Arguments.Add(new InvokeArgument(argName1, arg1));

			Assert.Throws<ArgumentException>(() => invoke.Arguments.Add(new InvokeArgument(arg2)));
		}

		[Fact]
		public static void SetAnonArgumentToNamedContext() {
			var name = ObjectName.Parse("sys.func1");
			var arg1 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(3452)));
			var arg2 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(false)));

			var invoke = new Invoke(name);
			invoke.Arguments.Add(new InvokeArgument("a", arg1));
			invoke.Arguments.Add(new InvokeArgument("b", arg2));

			Assert.Equal(name, invoke.MethodName);
			Assert.Equal(2, invoke.Arguments.Count);
			Assert.True(invoke.IsNamed);
			Assert.True(invoke.Arguments[0].IsNamed);
			Assert.True(invoke.Arguments[1].IsNamed);

			Assert.Throws<ArgumentException>(() => invoke.Arguments[0] = new InvokeArgument(invoke.Arguments[0].Value));
		}
	}
}