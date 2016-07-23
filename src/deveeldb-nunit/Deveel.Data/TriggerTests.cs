using System;
using System.Linq;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class TriggerTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			CreateTestTable(query, tableName);
			CreateTriggers(query, tableName);
			CreateProcedureTrigger(query, tableName);

			return true;
		}

		private void CreateTestTable(IQuery query, ObjectName tableName) {
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.VarChar());
			query.Access().CreateObject(tableInfo);
		}

		private void CreateTriggers(IQuery query, ObjectName tableName) {
			var triggerName = ObjectName.Parse("APP.trigger1");
			var eventTime = TriggerEventTime.Before;
			var eventType = TriggerEventType.Insert | TriggerEventType.Update;
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(33)));
			var triggerInfo = new PlSqlTriggerInfo(triggerName, tableName, eventTime, eventType, body);
			query.Access().CreateObject(triggerInfo);
		}

		private void CreateProcedureTrigger(IQuery query, ObjectName tableName) {
			var procedurName = ObjectName.Parse("APP.proc1");
			var body = new PlSqlBlockStatement();
			body.Declarations.Add(new DeclareVariableStatement("a", PrimitiveTypes.Integer()));
			body.Statements.Add(new AssignVariableStatement(SqlExpression.VariableReference("a"), SqlExpression.Constant(33)));
			var procedureInfo = new PlSqlProcedureInfo(procedurName, new RoutineParameter[0], body);

			query.Access().CreateObject(procedureInfo);

			var triggerName = ObjectName.Parse("APP.trigger2");
			var eventTime = TriggerEventTime.After;
			var eventType = TriggerEventType.Insert | TriggerEventType.Update;

			var triggerInfo = new ProcedureTriggerInfo(triggerName, tableName, eventTime, eventType, procedurName);
			query.Access().CreateObject(triggerInfo);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var triggerName1 = ObjectName.Parse("APP.trigger1");
			var triggerName2 = ObjectName.Parse("APP.trigger2");
			var tableName = ObjectName.Parse("APP.test_table");
			var procedurName = ObjectName.Parse("APP.proc1");

			query.Access().DropObject(DbObjectType.Trigger, triggerName1);
			query.Access().DropObject(DbObjectType.Trigger, triggerName2);
			query.Access().DropObject(DbObjectType.Table, tableName);
			query.Access().DropObject(DbObjectType.Routine, procedurName);

			return true;
		}

		[Test]
		public void BeforeInsert() {
			var tableName = ObjectName.Parse("APP.test_table");

			var reset = new AutoResetEvent(false);

			TriggerEvent firedEvent = null;
			Query.Context.RouteImmediate<TriggerEvent>(e => {
				firedEvent = e;
				reset.Set();
			}, e => e.TriggerType == TriggerType.Procedural &&
			        e.TriggerName.FullName.Equals("APP.trigger1"));

			Query.Insert(tableName, new[] {"id", "name"},
				new SqlExpression[] {SqlExpression.Constant(2), SqlExpression.Constant("The Name")});

			reset.WaitOne(500);

			Assert.IsNotNull(firedEvent);
		}

		[Test]
		public void AfterInsert() {
			var tableName = ObjectName.Parse("APP.test_table");

			var reset = new AutoResetEvent(false);

			TriggerEvent firedEvent = null;
			Query.Context.RouteImmediate<TriggerEvent>(e => {
				firedEvent = e;
				reset.Set();
			}, e => {
				return e.TriggerType == TriggerType.External &&
				       e.TriggerName.FullName.Equals("APP.trigger2");
			});

			Query.Insert(tableName, new[] { "id", "name" },
				new SqlExpression[] { SqlExpression.Constant(2), SqlExpression.Constant("The Name") });

			reset.WaitOne(500);

			Assert.IsNotNull(firedEvent);
		}

		[Test]
		public void Callback() {
			var tableName = ObjectName.Parse("APP.test_table");

			Query.Access().CreateCallbackTrigger("callback1", tableName, TriggerEventTime.After, TriggerEventType.Insert);

			var reset = new AutoResetEvent(false);

			TriggerEvent firedEvent = null;
			Query.Context.RouteImmediate<TriggerEvent>(e => {
				firedEvent = e;
				reset.Set();
			}, e => e.TriggerType == TriggerType.Callback &&
					e.TriggerName.Name.Equals("callback1"));

			Query.Insert(tableName, new[] { "id", "name" },
				new SqlExpression[] { SqlExpression.Constant(2), SqlExpression.Constant("The Name") });

			reset.WaitOne(300);

			Assert.IsNotNull(firedEvent);
			Assert.AreEqual(TriggerEventTime.After, firedEvent.EventTime);
			Assert.AreEqual(TriggerEventType.Insert, firedEvent.EventType);
		}

		[Test]
		public void SelectTriggerInfo() {
			const string sql = "SELECT type FROM trigger1";
			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			var result = Query.Select(query);

			Row row = null;
			Assert.IsNotNull(result);
			Assert.DoesNotThrow(() => row = result.First());
			Assert.IsNotNull(row);

			var value = row.GetValue("type");
			Assert.IsFalse(Field.IsNullField(value));
		}
	}
}
