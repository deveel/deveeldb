using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropTriggerTests : SqlCompileTestBase {
		[Test]
		public void DropProcedureTrigger() {
			const string sql = "DROP TRIGGER trigger1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropTriggerStatement>(statement);

			var dropTrigger = (DropTriggerStatement) statement;
			var triggerName = new ObjectName("trigger1");

			Assert.AreEqual(triggerName, dropTrigger.TriggerName);
		}

		[Test]
		public void DropCallbackTrigger() {
			const string sql = "DROP CALLBACK TRIGGER trigger1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropCallbackTriggersStatement>(statement);

			var dropTrigger = (DropCallbackTriggersStatement)statement;

			Assert.AreEqual("trigger1", dropTrigger.TriggerName);
		}
	}
}