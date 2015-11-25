using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Triggers {
	[TestFixture]
	public class TriggerTests : ContextBasedTest {
		private static readonly ObjectName TestTableName = ObjectName.Parse("APP.test_table");

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("date", PrimitiveTypes.DateTime());
			query.CreateTable(tableInfo);
			return query;
		}

		[Test]
		public void CreateCallbackTrigger() {
			var triggerName = ObjectName.Parse("APP.test_trigger");
			Assert.DoesNotThrow(() => Query.CreateCallbackTrigger(triggerName, TriggerEventType.BeforeInsert));

			bool exists = false;
			Assert.DoesNotThrow(() => exists = Query.TriggerExists(triggerName));
			Assert.IsTrue(exists);
		}

		[Test]
		public void CreateProcedureTrigger() {
			
		}
	}
}
