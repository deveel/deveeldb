// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

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
			var args = new Field[] {Field.BigInt(2), Field.Number(new SqlNumber(54))};
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
			protected override void OnInit() {
				Register(config => config.Named("user2")
					.ReturnsType(PrimitiveTypes.String())
					.WhenExecute(context => context.Result(Field.String(context.Request.User().Name))));
			}
		}

		#endregion

		#region Factory2

		class Factory2 : FunctionProvider {
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
