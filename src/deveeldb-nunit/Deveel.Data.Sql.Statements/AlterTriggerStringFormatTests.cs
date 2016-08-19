using System;

using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class AlterTriggerStringFormatTests {
		[Test]
		public static void RenameTo() {
			var statement = new AlterTriggerStatement(ObjectName.Parse("APP.trig1"), new RenameTriggerAction(ObjectName.Parse("APP.trig1_bak")));

			var expected = "ALTER TRIGGER APP.trig1 RENAME TO APP.trig1_bak";
			Assert.AreEqual(expected, statement.ToString());
		}

		[Test]
		public static void Enable() {
			var statement = new AlterTriggerStatement(ObjectName.Parse("APP.trig2"), new ChangeTriggerStatusAction(TriggerStatus.Enabled));

			var expected = "ALTER TRIGGER APP.trig2 ENABLE";
			Assert.AreEqual(expected, statement.ToString());
		}

		[Test]
		public static void Disable() {
			var statement = new AlterTriggerStatement(ObjectName.Parse("APP.trig2"), new ChangeTriggerStatusAction(TriggerStatus.Disabled));

			var expected = "ALTER TRIGGER APP.trig2 DISABLE";
			Assert.AreEqual(expected, statement.ToString());
		}
	}
}
