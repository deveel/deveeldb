using System;

using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Triggers {
	[TestFixture]
	public class TriggerTests : ContextBasedTest {
		private static readonly ObjectName TestTableName = new ObjectName("APP.test_table");

		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var context = base.CreateQueryContext(database);

			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("date", PrimitiveTypes.DateTime());

			return context;
		}

		[Test]
		public void CreateCallbackTrigger() {
			var triggerName = ObjectName.Parse("APP.test_trigger");
			Assert.DoesNotThrow(() => QueryContext.CreateCallbackTrigger(triggerName, TriggerEventType.BeforeInsert));

			bool exists = false;
			Assert.DoesNotThrow(() => exists = QueryContext.TriggerExists(triggerName));
			Assert.IsTrue(exists);
		}

		[Test]
		public void CreateProcedureTrigger() {
			
		}
	}
}
