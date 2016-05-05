using System;
using System.Threading;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public sealed class LoggerTests : ContextBasedTest {
		[Test]
		public void LogToConsole() {
			Query.AsEventSource().UseLogger(new ConsoleLogger());
			Query.CreateTable(new ObjectName("test_table"), new []{new SqlTableColumn("a", PrimitiveTypes.String())});
		}

		[Test]
		public void BaseLogger() {
			string logged = null;

			var reset = new AutoResetEvent(false);
			Query.AsEventSource().UseLogger(new TestLogger(reset, message => logged = message));
			Query.Context.OnDebug("Message to debug");
			reset.WaitOne(1000);
			Assert.IsNotNull(logged);

			// TODO: the format of the message is still an issue
		}

		#region TestLogger

		class TestLogger : LoggerBase {
			private Action<string> logCallback;
			private AutoResetEvent reset;

			public TestLogger(AutoResetEvent reset, Action<string> logCallback) {
				this.reset = reset;
				this.logCallback = logCallback;
			}

			protected override void LogMessage(string message) {
				logCallback(message);
				reset.Set();
			}
		}

		#endregion
	}
}
