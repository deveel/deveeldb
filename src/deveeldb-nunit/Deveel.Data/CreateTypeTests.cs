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
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateTypeTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			if (testName == "UnderType") {
				var typeName = ObjectName.Parse("APP.parentType");
				var typeInfo = new UserTypeInfo(typeName);
				typeInfo.AddMember("id", PrimitiveTypes.Integer());
				typeInfo.AddMember("name", PrimitiveTypes.String());

				Query.Access().CreateObject(typeInfo);
			}

			base.OnAfterSetup(testName);
		}

		[Test]
		public void SimpleType() {
			var typeName = ObjectName.Parse("APP.type1");
			Query.CreateType(typeName, new[] {new UserTypeMember("a", PrimitiveTypes.String())});

			var exists = Query.Access().TypeExists(typeName);

			Assert.IsTrue(exists);
		}

		[Test]
		public void UnderType() {
			var typeName = ObjectName.Parse("APP.type1");
			var parentTypeName = ObjectName.Parse("APP.parentType");

			Query.CreateType(typeName, parentTypeName, new UserTypeMember("age", PrimitiveTypes.Integer()));

			var exists = Query.Access().TypeExists(typeName);

			Assert.IsTrue(exists);
		}

		[Test]
		public void UnderNotExistingType() {
			var typeName = ObjectName.Parse("APP.type1");
			var parentTypeName = ObjectName.Parse("APP.parentType");

			Assert.Throws<StatementException>(() => Query.CreateType(typeName, parentTypeName, new UserTypeMember("age", PrimitiveTypes.Integer())));
		}
	}
}
