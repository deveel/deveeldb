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
