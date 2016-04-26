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

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropFunctionTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var funcName = ObjectName.Parse("APP.func1");
			var returnType = PrimitiveTypes.String();
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new ReturnStatement(SqlExpression.Constant("Hello!")));

			var funtionInfo = new PlSqlFunctionInfo(funcName, new RoutineParameter[0], returnType, body);
			query.Access().CreateObject(funtionInfo);

			return true;
		}

		protected override void OnBeforeTearDown(string testName) {
			if (testName == "Existing")
				base.OnBeforeTearDown(testName);
		}

		[Test]
		public void Existing() {
			var funcName = ObjectName.Parse("APP.func1");

			Query.DropFunction(funcName);

			var exists = Query.Access().ObjectExists(DbObjectType.Routine, funcName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void NotExisting() {
			var funcName = ObjectName.Parse("APP.func2");

			Assert.Throws<ObjectNotFoundException>(() => Query.DropFunction(funcName));
		}

		[Test]
		public void NotExisting_IfExistsClause() {
			var funcName = ObjectName.Parse("APP.func2");

			Assert.DoesNotThrow(() => Query.DropFunction(funcName, true));
		}
	}
}
