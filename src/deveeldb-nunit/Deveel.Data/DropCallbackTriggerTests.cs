using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropCallbackTriggerTests : ContextBasedTest {
		private string triggerName;

		protected override void OnAfterSetup(string testName) {
			triggerName = "trigger1";
			Query.Access().CreateCallbackTrigger(triggerName, ObjectName.Parse("APP.table1"),
				TriggerEventTime.After, TriggerEventType.Delete);
		}

		[Test]
		public void Existing() {
			Query.DropCallbackTrigger(triggerName);

			var exists = Query.Access().TriggerExists(new ObjectName(triggerName));
			Assert.IsFalse(exists);
		}
	}
}
