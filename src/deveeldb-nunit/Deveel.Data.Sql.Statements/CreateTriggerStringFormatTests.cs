using System;
using System.Text;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Triggers;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CreateTriggerStringFormatTests {
		[Test]
		public static void TriggerWithBody() {
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new OpenStatement("c1"));
			body.Statements.Add(new CursorForLoopStatement("i", "c1"));
			body.Statements.Add(new ReturnStatement());

			var statement = new CreateTriggerStatement(ObjectName.Parse("APP.trig1"), new ObjectName("tab1"), body,
				TriggerEventTime.Before, TriggerEventType.Insert | TriggerEventType.Update);

			var expected = new StringBuilder();
			expected.AppendLine("CREATE TRIGGER APP.trig1 BEFORE INSERT OR UPDATE ON tab1");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    OPEN c1");
			expected.AppendLine("    FOR i IN c1");
			expected.AppendLine("    LOOP");
			expected.AppendLine("    END LOOP");
			expected.AppendLine("    RETURN");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), statement.ToString());
		}

		[Test]
		public static void TriggerWithBody_InitiallyDisabled() {
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new OpenStatement("c1"));
			body.Statements.Add(new CursorForLoopStatement("i", "c1"));
			body.Statements.Add(new ReturnStatement());

			var statement = new CreateTriggerStatement(ObjectName.Parse("APP.trig1"), new ObjectName("tab1"), body,
				TriggerEventTime.Before, TriggerEventType.Insert | TriggerEventType.Update) {
					Status = TriggerStatus.Disabled,
					ReplaceIfExists = true
				};

			var expected = new StringBuilder();
			expected.AppendLine("CREATE OR REPLACE TRIGGER APP.trig1 BEFORE INSERT OR UPDATE ON tab1 DISABLE");
			expected.AppendLine("  BEGIN");
			expected.AppendLine("    OPEN c1");
			expected.AppendLine("    FOR i IN c1");
			expected.AppendLine("    LOOP");
			expected.AppendLine("    END LOOP");
			expected.AppendLine("    RETURN");
			expected.Append("  END");

			Assert.AreEqual(expected.ToString(), statement.ToString());
		}

		[Test]
		public static void ProcedureTrigger_NoArguments() {
			var statement = new CreateProcedureTriggerStatement(ObjectName.Parse("APP.trig1"), new ObjectName("tab1"), 
				ObjectName.Parse("APP.proc1"), TriggerEventTime.After, TriggerEventType.Delete);

			var expected = "CREATE TRIGGER APP.trig1 AFTER DELETE ON tab1 FOR EACH ROW CALL APP.proc1()";

			Assert.AreEqual(expected, statement.ToString());
		}

		[Test]
		public static void ProcedureTrigger_WithArguments() {
			var args = new InvokeArgument[] {
				new InvokeArgument("a", SqlExpression.Constant(3))
			};

			var statement = new CreateProcedureTriggerStatement(ObjectName.Parse("APP.trig1"), new ObjectName("tab1"),
				ObjectName.Parse("APP.proc1"), args, TriggerEventTime.After, TriggerEventType.Delete) {
					ReplaceIfExists = true,
					Status = TriggerStatus.Enabled
				};

			var expected = "CREATE OR REPLACE TRIGGER APP.trig1 AFTER DELETE ON tab1 FOR EACH ROW ENABLE CALL APP.proc1(a => 3)";

			Assert.AreEqual(expected, statement.ToString());
		}
	}
}
