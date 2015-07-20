using System;

namespace Deveel.Data.Diagnostics {
	public class ConsoleEventLogger : EventLoggerBase {
		protected override void WriteToLog(string message) {
			Console.Out.WriteLine(message);
		}
	}
}
