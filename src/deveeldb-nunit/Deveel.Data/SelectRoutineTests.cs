using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectRoutineTests : ContextBasedTest {
		private ObjectName procName;
		private ObjectName funcName;

		protected override bool OnSetUp(string testName, IQuery query) {
			procName = ObjectName.Parse("APP.proc1");
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new CallStatement(ObjectName.Parse("proc2")));
			var procInfo = new PlSqlProcedureInfo(procName, new RoutineParameter[0], body);
			query.Access().CreateObject(procInfo);

			funcName = ObjectName.Parse("APP.func1");
			body = new PlSqlBlockStatement();
			body.Statements.Add(new ReturnStatement(SqlExpression.Constant(22)));
			var funcInfo = new PlSqlFunctionInfo(funcName, new RoutineParameter[0], PrimitiveTypes.Integer(), body);
			query.Access().CreateObject(funcInfo);

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Routine, procName);
			query.Access().DropObject(DbObjectType.Routine, funcName);

			return true;
		}

		[Test]
		public void SelectAllFromProcedure() {
			const string sql = "SELECT * FROM proc1";

			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			Assert.IsNotNull(query);

			var result = Query.Select(query);

			Row row = null;

			Assert.IsNotNull(result);
			Assert.DoesNotThrow(() => row = result.Fetch(FetchDirection.Next, -1));

			Assert.IsNotNull(row);
		}
	}
}
