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

			// TODO: assert it exists
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

			// TODO: assert it exists
		}

		static class Test {
			public static int Function(int a, int b) {
				return a + b;
			}
		}
	}
}
