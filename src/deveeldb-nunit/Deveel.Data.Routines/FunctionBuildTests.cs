using System;

using Deveel.Data;
using Deveel.Data.Sql.Fluid;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public class FunctionBuildTests : ContextBasedTest {
		[Test]
		public void ScalarWithNoArguments() {
			FunctionProvider factory1 = null;
			Assert.DoesNotThrow(() => factory1 = new Factory1());

			IFunction function = null;
			Assert.DoesNotThrow(() => function = factory1.ResolveFunction("user2"));
			Assert.IsNotNull(function);

			InvokeResult result=null;
			Assert.DoesNotThrow(() => result = function.Execute(Query));
			Assert.IsNotNull(result);
			Assert.AreEqual(AdminUserName, result.ReturnValue.Value.ToString());
		}

		[Test]
		public void ScalarWithTwoArgument() {
			Factory2 factory2 = null;
			Assert.DoesNotThrow(() => factory2 = new Factory2());

			IFunction function = null;
			var args = new DataObject[] {DataObject.BigInt(2), DataObject.Number(new SqlNumber(54))};
			Assert.DoesNotThrow(() => function = factory2.ResolveFunction("add2", args));
			Assert.IsNotNull(function);

			InvokeResult result = null;
			Assert.DoesNotThrow(() => result = function.Execute(args));
			Assert.IsNotNull(result);

			Assert.IsInstanceOf<SqlNumber>(result.ReturnValue.Value);

			var value = ((SqlNumber) result.ReturnValue.Value).ToInt64();
			Assert.AreEqual(56, value);
		}

		#region Factory1

		class Factory1 : FunctionProvider {
			public override string SchemaName {
				get { return "APP"; }
			}

			protected override void OnInit() {
				Register(config => config.Named("user2")
					.ReturnsType(PrimitiveTypes.String())
					.WhenExecute(context => context.Result(DataObject.String(context.Request.User().Name))));
			}
		}

		#endregion

		#region Factory2

		class Factory2 : FunctionProvider {
			public override string SchemaName {
				get { return "APP"; }
			}

			protected override void OnInit() {
				Register(config => config.Named("add2")
					.WithNumericParameter("a")
					.WithNumericParameter("b")
					.ReturnsNumeric()
					.WhenExecute(context => {
						var a = context.EvaluatedArguments[0];
						var b = context.EvaluatedArguments[1];
						return context.Result(a.Add(b));
					}));
			}
		}

		#endregion
	}
}
