using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CallTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var procName = ObjectName.Parse("APP.proc1");
			var args = new [] { new RoutineParameter("a", PrimitiveTypes.String()) };
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("b", PrimitiveTypes.String()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("b"), SqlExpression.VariableReference("a")));

			var procInfo = new PlSqlProcedureInfo(procName, args, body);
			query.Access().CreateObject(procInfo);

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var procName = ObjectName.Parse("APP.proc1");
			query.Access().DropObject(DbObjectType.Routine, procName);
			return true;
		}

		[Test]
		public void WithArguments() {
			var procName = ObjectName.Parse("APP.proc1");
			var arg = SqlExpression.Constant("Hello!");

			Query.Call(procName, arg);
		}
	}
}
