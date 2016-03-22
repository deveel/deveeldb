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
			Query.CreateFunction(funName, PrimitiveTypes.Numeric(), body);

			var exists = Query.Access.RoutineExists(funName);

			Assert.IsTrue(exists);

			var function = Query.Access.GetObject(DbObjectType.Routine, funName);
			Assert.IsNotNull(function);
			Assert.IsInstanceOf<UserFunction>(function);

			var userFunction = (UserFunction) function;
			Assert.IsNotNull(userFunction.FunctionInfo.Body);
		}

		[Test]
		public void CreateExternalFunction() {
			var funName = ObjectName.Parse("APP.fun1");
			var parameters = new RoutineParameter[] {
				new RoutineParameter("a", PrimitiveTypes.Integer()),
				new RoutineParameter("b", PrimitiveTypes.Integer()),
			};

			var externRef = ExternalRef.MakeRef(typeof(Test), "Function(int, int)");
			Query.CreateExternFunction(funName, PrimitiveTypes.Integer(), parameters, externRef.ToString());

			var exists = Query.Access.RoutineExists(funName);

			Assert.IsTrue(exists);

			var function = Query.Access.GetObject(DbObjectType.Routine, funName);

			Assert.IsNotNull(function);
			Assert.IsInstanceOf<ExternalFunction>(function);

			var externFunction = (ExternalFunction) function;
			Assert.IsNotNull(externFunction.FunctionInfo.ExternalRef);
			Assert.AreEqual(typeof(Test), externFunction.FunctionInfo.ExternalRef.Type);
		}

		static class Test {
			public static int Function(int a, int b) {
				return a + b;
			}
		}
	}
}
