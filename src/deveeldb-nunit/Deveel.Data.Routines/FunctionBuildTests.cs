using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Fluid;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using Moq;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public class FunctionBuildTests {
		private IQueryContext NewUserQueryContext(User user) {
			var sessionMock = new Mock<IUserSession>();
			sessionMock.Setup(x => x.User)
				.Returns(user);
			var queryContextMock = new Mock<IQueryContext>();
			queryContextMock.Setup(x => x.Session)
				.Returns(sessionMock.Object);
			return queryContextMock.Object;
		}

		[Test]
		public void ScalarWithNoArguments() {
			var factory1 = new Factory1();
			Assert.DoesNotThrow(factory1.Init);

			IFunction function = null;
			Assert.DoesNotThrow(() => function = factory1.ResolveFunction("user"));
			Assert.IsNotNull(function);

			ExecuteResult result=null;
			var queryContext = NewUserQueryContext(User.System);
			Assert.DoesNotThrow(() => result = function.Execute(queryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(User.System.Name, result.ReturnValue.Value.ToString());
		}

		[Test]
		public void ScalarWithTwoArgument() {
			var factory2 = new Factory2();
			Assert.DoesNotThrow(factory2.Init);

			IFunction function = null;
			var args = new DataObject[] {DataObject.BigInt(2), DataObject.Number(new SqlNumber(54))};
			Assert.DoesNotThrow(() => function = factory2.ResolveFunction("add", args));
			Assert.IsNotNull(function);

			ExecuteResult result = null;
			Assert.DoesNotThrow(() => result = function.Execute(args));
			Assert.IsNotNull(result);

			Assert.IsInstanceOf<SqlNumber>(result.ReturnValue.Value);

			var value = ((SqlNumber) result.ReturnValue.Value).ToInt64();
			Assert.AreEqual(56, value);
		}

		#region Factory1

		class Factory1 : FunctionFactory {
			public override string SchemaName {
				get { return "APP"; }
			}

			protected override void OnInit() {
				New("user")
					.ReturnsType(PrimitiveTypes.String())
					.WhenExecute(context => context.Result(DataObject.String(context.QueryContext.User().Name)));
			}
		}

		#endregion

		#region Factory2

		class Factory2 : FunctionFactory {
			public override string SchemaName {
				get { return "APP"; }
			}

			protected override void OnInit() {
				New("add")
					.WithNumericParameter("a")
					.WithNumericParameter("b")
					.ReturnsNumeric()
					.WhenExecute(context => {
						var a = context.EvaluatedArguments[0];
						var b = context.EvaluatedArguments[1];
						return context.Result(a.Add(b));
					});
			}
		}

		#endregion
	}
}
