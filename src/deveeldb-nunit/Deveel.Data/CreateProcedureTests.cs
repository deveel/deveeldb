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
	public sealed class CreateProcedureTests : ContextBasedTest {
		[Test]
		public void CreateExternalProcedure() {
			var procName = ObjectName.Parse("APP.proc1");
			var parameters = new RoutineParameter[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()),
				new RoutineParameter("b", PrimitiveTypes.Integer()),
			};

			var externRef = ExternalRef.MakeRef(typeof(Test), "Procedure(int, int)");
			AdminQuery.CreateExternProcedure(procName, parameters, externRef.ToString());

			var exists = AdminQuery.Access().RoutineExists(procName);

			Assert.IsTrue(exists);

			var procedure = AdminQuery.Access().GetObject(DbObjectType.Routine, procName);

			Assert.IsNotNull(procedure);
			Assert.IsInstanceOf<ExternalProcedure>(procedure);

			var externFunction = (ExternalProcedure)procedure;
			Assert.IsNotNull(externFunction.ExternalRef);
			Assert.AreEqual(typeof(Test), externFunction.ExternalRef.Type);
		}

		[Test]
		public void SimpleCallFunction() {
			var block = new PlSqlBlockStatement();
			block.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.String()));
			block.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.FunctionCall("user")));

			var procName = ObjectName.Parse("APP.proc1");
			AdminQuery.CreateProcedure(procName, block);

			var exists = AdminQuery.Access().RoutineExists(procName);

			Assert.IsTrue(exists);

			var procedure = AdminQuery.Access().GetObject(DbObjectType.Routine, procName);

			Assert.IsNotNull(procedure);
			Assert.IsInstanceOf<Procedure>(procedure);
		}

		static class Test {
			public static int Result;

			public static void Procedure(int a, int b) {
				Result = a + b;
			}
		}
	}
}
