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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class AssignTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			AdminQuery.Access().CreateObject(new VariableInfo("a", PrimitiveTypes.Integer(), false));
			AdminQuery.Access().CreateObject(new VariableInfo("b", PrimitiveTypes.Integer(), true) {
				DefaultExpression = SqlExpression.Constant(56)
			});
		}

		protected override void OnBeforeTearDown(string testName) {
			if (testName != "ToConstant")
				base.OnBeforeTearDown(testName);
		}

		[Test]
		public void Existing() {
			var result = AdminQuery.Assign("a", SqlExpression.Constant(7689));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);

			var variable = AdminQuery.Context.FindVariable("a");
			Assert.IsNotNull(variable);
			Assert.IsNotNull(variable.Expression);
			Assert.IsNotNull(variable.Evaluate(AdminQuery));

			var value = ((SqlNumber) variable.Evaluate(AdminQuery).Value).ToDouble();
			Assert.AreEqual(7689, value);
		}

		[Test]
		public void NotExisting() {
			var result = AdminQuery.Assign("c", SqlExpression.Constant(7689));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);

			var variable = AdminQuery.Context.FindVariable("c");
			Assert.IsNotNull(variable);
			Assert.IsNotNull(variable.Expression);
			Assert.IsNotNull(variable.Evaluate(AdminQuery));

			var value = ((SqlNumber)variable.Evaluate(AdminQuery).Value).ToDouble();
			Assert.AreEqual(7689, value);
		}

		[Test]
		public void ToConstant() {
			Assert.Throws<ConstantVariableViolationException>(() => AdminQuery.Assign("b", SqlExpression.Constant(453)));
		}
	}
}
