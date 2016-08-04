using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class AlterTriggerTests : ContextBasedTest {
		private ObjectName triggerName;

		protected override bool OnSetUp(string testName, IQuery query) {
			triggerName = ObjectName.Parse("APP.trigger1");
			query.Access().CreateTrigger(new ProcedureTriggerInfo(triggerName, ObjectName.Parse("APP.table1"),
				TriggerEventTime.After, TriggerEventType.Delete, ObjectName.Parse("APP.proc1")));

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropTrigger(triggerName);
			return true;
		}

		[Test]
		public void RenameTo() {
			var newName = ObjectName.Parse("APP.trigger2");

			AdminQuery.RenameTrigger(triggerName, newName);

			Assert.IsFalse(AdminQuery.Access().TriggerExists(triggerName));
			Assert.IsTrue(AdminQuery.Access().TriggerExists(newName));
		}

		[Test]
		public void Enable() {
			AdminQuery.EnableTrigger(triggerName);

			var trigger = (Trigger) AdminQuery.Access().GetObject(DbObjectType.Trigger, triggerName);
			Assert.AreEqual(TriggerStatus.Enabled, trigger.TriggerInfo.Status);
		}

		[Test]
		public void Disable() {
			AdminQuery.DisableTrigger(triggerName);

			var trigger = (Trigger)AdminQuery.Access().GetObject(DbObjectType.Trigger, triggerName);
			Assert.AreEqual(TriggerStatus.Disabled, trigger.TriggerInfo.Status);
		}
	}
}
