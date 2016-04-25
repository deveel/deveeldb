using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CallTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateProcedure1(query);
			CreateProcedure2(query);
			CreateProcedure3(query);
			return true;
		}

		private void CreateProcedure1(IQuery query) {
			var procName = ObjectName.Parse("APP.proc1");
			var args = new[] { new RoutineParameter("a", PrimitiveTypes.String()) };
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("b", PrimitiveTypes.String()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("b"), SqlExpression.VariableReference("a")));

			var procInfo = new PlSqlProcedureInfo(procName, args, body);
			query.Access().CreateObject(procInfo);
		}

		private void CreateProcedure2(IQuery query) {
			var procName = ObjectName.Parse("APP.proc2");
			var args = new[] {
				new RoutineParameter("a", PrimitiveTypes.String()),
				new RoutineParameter("b", PrimitiveTypes.String()) 
			};
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("c", PrimitiveTypes.String()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("c"), SqlExpression.VariableReference("a")));

			var procInfo = new PlSqlProcedureInfo(procName, args, body);
			query.Access().CreateObject(procInfo);
		}

		private void CreateProcedure3(IQuery query) {
			var procName = ObjectName.Parse("APP.proc3");
			var args = new[] {
				new RoutineParameter("a", PrimitiveTypes.String()),
				new RoutineParameter("b", PrimitiveTypes.String(), ParameterDirection.Output)
			};
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("c", PrimitiveTypes.String()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("c"), SqlExpression.VariableReference("a")));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("b"), SqlExpression.VariableReference("c")));

			var procInfo = new PlSqlProcedureInfo(procName, args, body);
			query.Access().CreateObject(procInfo);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Routine, ObjectName.Parse("APP.proc1"));
			query.Access().DropObject(DbObjectType.Routine, ObjectName.Parse("APP.proc2"));
			query.Access().DropObject(DbObjectType.Routine, ObjectName.Parse("APP.proc3"));
			return true;
		}

		[Test]
		public void WithArgument() {
			var procName = ObjectName.Parse("APP.proc1");
			var arg = SqlExpression.Constant("Hello!");

			Query.Call(procName, arg);
		}

		[Test]
		public void WithNamedArgument() {
			var procName = ObjectName.Parse("APP.proc1");
			var arg = new InvokeArgument("a", SqlExpression.Constant("Hello!"));

			Query.Call(procName, arg);
		}

		[Test]
		public void WithArguments() {
			var procName = ObjectName.Parse("APP.proc2");
			var arg1 = SqlExpression.Constant("Hello");
			var arg2 = SqlExpression.Constant("World!");

			Query.Call(procName, arg1, arg2);
		}

		[Test]
		public void WithNamedArguments() {
			var procName = ObjectName.Parse("APP.proc2");
			var arg1 = new InvokeArgument("a", SqlExpression.Constant("Hello"));
			var arg2 = new InvokeArgument("b", SqlExpression.Constant("World!"));

			Query.Call(procName, arg1, arg2);
		}

		[Test]
		public void WithOutputArgument() {
			var procName = ObjectName.Parse("APP.proc3");
			var arg = new InvokeArgument("a", SqlExpression.Constant("Hello"));

			var result = Query.Call(procName, arg);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.Count);

			Field value;
			Assert.IsTrue(result.TryGetValue("b", out value));
			Assert.IsFalse(Field.IsNullField(value));

			Assert.AreEqual("Hello", ((SqlString)value.Value).ToString());
		}
	}
}
