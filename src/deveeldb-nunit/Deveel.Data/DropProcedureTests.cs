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

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropProcedureTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var procName = ObjectName.Parse("APP.proc1");
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(34)));

			var funtionInfo = new PlSqlProcedureInfo(procName, new RoutineParameter[0], body);
			query.Access().CreateObject(funtionInfo);

			return true;
		}

		[Test]
		public void Existing() {
			var procName = ObjectName.Parse("APP.proc1");

			Query.DropProcedure(procName);

			var exists = Query.Access().ObjectExists(DbObjectType.Routine, procName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void NotExisting() {
			var procName = ObjectName.Parse("APP.proc1");

			Assert.Throws<ObjectNotFoundException>(() => Query.DropProcedure(procName));
		}

		[Test]
		public void NotExisting_IfExistsClause() {
			var procName = ObjectName.Parse("APP.proc1");

			Assert.DoesNotThrow(() => Query.DropProcedure(procName, true));
		}
	}
}
