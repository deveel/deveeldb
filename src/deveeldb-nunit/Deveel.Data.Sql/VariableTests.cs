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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class VariableTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			if (testName != "DeclareVariable")
				Query.DeclareVariable("a", PrimitiveTypes.String());

			base.OnSetUp(testName);
		}

		[Test]
		public void DeclareVariable() {
			Query.DeclareVariable("a", PrimitiveTypes.String());
		}

		[Test]
		public void GetVariable() {
			var variable = Query.FindVariable("a");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<StringType>(variable.Type);
		}

		[Test]
		public void SetExistingVariable() {
			Query.SetVariable("a", SqlExpression.Constant("test"));

			var variable = Query.FindVariable("a");
			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<StringType>(variable.Type);
			Assert.IsNotNull(variable.Value);
			Assert.IsInstanceOf<StringType>(variable.Value.Type);
		}

		[Test]
		public void SetNotExistingVariable() {
			Query.SetVariable("b", SqlExpression.Constant(23));

			var variable = Query.FindVariable("b");
			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsNotNull(variable.Value);
			Assert.IsInstanceOf<NumericType>(variable.Value.Type);
		}
	}
}
