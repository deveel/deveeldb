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
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DeclareVariableTests : ContextBasedTest {
		[Test]
		public void SimpleVariableInQueryContext() {
			Query.DeclareVariable("a", PrimitiveTypes.String());

			var obj = Query.Access().GetObject(DbObjectType.Variable, new ObjectName("a"));

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<Variable>(obj);

			var variable = (Variable) obj;

			Assert.AreEqual("a", variable.Name);
			Assert.IsInstanceOf<StringType>(variable.Type);
		}
	}
}
