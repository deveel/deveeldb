using System;
using System.Linq;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class AlterTriggerTests : SqlCompileTestBase {
		[TestCase("ENABLE", TriggerStatus.Enabled)]
		[TestCase("DISABLE", TriggerStatus.Disabled)]
		public void ChangeStatus(string newStatus, TriggerStatus expectedStatus) {
			var sql = String.Format("ALTER TRIGGER trig1 {0}", newStatus);

			var result = Compile(sql);
			
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AlterTriggerStatement>(statement);

			var alterTrigger = (AlterTriggerStatement) statement;

			Assert.IsNotNull(alterTrigger.TriggerName);
			Assert.IsNotNull(alterTrigger.Action);
			Assert.IsInstanceOf<ChangeTriggerStatusAction>(alterTrigger.Action);

			var action = (ChangeTriggerStatusAction) alterTrigger.Action;

			Assert.AreEqual(expectedStatus, action);
		}

		[Test]
		public void RenameTo() {
			const string sql = "ALTER TRIGGER APP.t1 RENAME TO SYS.trig1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<AlterTriggerStatement>(statement);

			var alterTrigger = (AlterTriggerStatement)statement;

			Assert.IsNotNull(alterTrigger.TriggerName);
			Assert.IsNotNull(alterTrigger.Action);
			Assert.IsInstanceOf<RenameTriggerAction>(alterTrigger.Action);

			var action = (RenameTriggerAction) alterTrigger.Action;

			Assert.IsNotNull(action.Name);
			Assert.AreEqual("SYS.trig1", action.Name.FullName);
		}
	}
}
