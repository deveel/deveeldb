using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateTriggerTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTable(Query);
		}

		private static void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
		}

		[Test]
		public void CallbackTrigger() {
			var tableName = ObjectName.Parse("APP.test_table");
			Query.CreateCallbackTrigger("trigger1", tableName, TriggerEventType.BeforeInsert);

			var trigger = Query.Access().GetObject(DbObjectType.Trigger, new ObjectName("trigger1")) as Trigger;

			Assert.IsNotNull(trigger);
			Assert.AreEqual("trigger1", trigger.TriggerInfo.TriggerName.FullName);
			Assert.AreEqual(tableName, trigger.TriggerInfo.TableName);
			Assert.AreEqual(TriggerEventType.BeforeInsert, trigger.TriggerInfo.EventTypes);
		}

		[Test]
		public void ProcedureTrigger_PlSql() {
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new CallStatement(ObjectName.Parse("system.output"), new[] {SqlExpression.Constant("One row was inserted")}));
			var triggerName = new ObjectName("trigger1");
			var tableName = ObjectName.Parse("APP.test_table");

			Query.CreateTrigger(triggerName, tableName, body, TriggerEventType.AfterInsert);

			var exists = Query.Access().TriggerExists(ObjectName.Parse("APP.trigger1"));

			Assert.IsTrue(exists);
		}
	}
}
