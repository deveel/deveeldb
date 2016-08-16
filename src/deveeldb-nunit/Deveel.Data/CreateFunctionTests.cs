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
	public sealed class CreateFunctionTests : ContextBasedTest {
		[Test]
		public void SimpleReturn() {
			var body = new PlSqlBlockStatement();

			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new ReturnStatement(SqlExpression.VariableReference("a")));

			var funName = ObjectName.Parse("APP.fun1");
			AdminQuery.CreateFunction(funName, PrimitiveTypes.Numeric(), body);

			var exists = AdminQuery.Access().RoutineExists(funName);

			Assert.IsTrue(exists);

			var function = AdminQuery.Access().GetObject(DbObjectType.Routine, funName);
			Assert.IsNotNull(function);
			Assert.IsInstanceOf<PlSqlFunction>(function);

			var userFunction = (PlSqlFunction) function;
			Assert.IsNotNull(userFunction.Body);
		}

		[Test]
		public void CreateExternalFunction() {
			var funName = ObjectName.Parse("APP.fun1");
			var parameters = new RoutineParameter[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()),
				new RoutineParameter("b", PrimitiveTypes.Integer()),
			};

			var externRef = ExternalRef.MakeRef(typeof(Test), "Function(int, int)");
			AdminQuery.CreateExternFunction(funName, PrimitiveTypes.Integer(), parameters, externRef.ToString());

			var exists = AdminQuery.Access().RoutineExists(funName);

			Assert.IsTrue(exists);

			var function = AdminQuery.Access().GetObject(DbObjectType.Routine, funName);

			Assert.IsNotNull(function);
			Assert.IsInstanceOf<ExternalFunction>(function);

			var externFunction = (ExternalFunction) function;
			Assert.IsNotNull(externFunction.ExternalRef);
			Assert.AreEqual(typeof(Test), externFunction.ExternalRef.Type);
		}

		static class Test {
			public static int Function(int a, int b) {
				return a + b;
			}
		}
	}
}
