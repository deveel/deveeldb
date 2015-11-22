using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Triggers {
	[TestFixture]
	public sealed class TriggerListenTests : ContextBasedTest {
		private static readonly ObjectName TestTableName = ObjectName.Parse("APP.test_table");

		protected override IQuery CreateQuery(IUserSession session) {
			var query = base.CreateQuery(session);

			var tableInfo = new TableInfo(TestTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());

			query.CreateTable(tableInfo);

			return query;
		}

		private TriggerEvent beforeEvent;
		private TriggerEvent afterEvent;

		protected override ISystemContext CreateSystemContext() {
			var context = base.CreateSystemContext();
			context.ListenTriggers(trigger => {
				if ((trigger.TriggerEventType & TriggerEventType.After) != 0) {
					afterEvent = trigger;
				} else if ((trigger.TriggerEventType & TriggerEventType.Before) != 0) {
					beforeEvent = trigger;
				}
			});
			return context;
		}

		protected override void OnSetUp(string testName) {
			if (!testName.EndsWith("_NoTriggers")) {
				// TODO: Create triggers
			}

			base.OnSetUp(testName);
		}

		[Test]
		public void Insert_NoTriggers() {
			var table = Query.GetMutableTable(TestTableName);

			Assert.IsNotNull(table);

			var row = table.NewRow();
			row.SetValue(0, 1);
			row.SetValue(1, "Antonello");
			row.SetValue(2, "Provenzano");

			Assert.DoesNotThrow(() => table.AddRow(row));
			Assert.DoesNotThrow(() => Query.Commit());

			Assert.IsNull(beforeEvent);
			Assert.IsNull(afterEvent);
		}
	}
}
