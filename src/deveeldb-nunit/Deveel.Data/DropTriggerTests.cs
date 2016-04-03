using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropTriggerTests : ContextBasedTest {
		private ObjectName triggerName;

		protected override void OnSetUp(string testName) {
			triggerName = ObjectName.Parse("APP.trigger1");
			Query.Access().CreateTrigger(new ProcedureTriggerInfo(triggerName, ObjectName.Parse("APP.table1"),
				TriggerEventType.AfterDelete, ObjectName.Parse("APP.proc1")));

			base.OnSetUp(testName);
		}

		protected override void OnTearDown() {
			base.OnTearDown();
		}

		[Test]
		public void ExistingTrigger() {
			Query.DropTrigger(triggerName);

			var exists = Query.Access().TriggerExists(triggerName);

			Assert.IsFalse(exists);
		}
	}
}
