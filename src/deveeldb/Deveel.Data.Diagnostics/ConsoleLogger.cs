using System;

namespace Deveel.Data.Diagnostics {
	public sealed class ConsoleLogger : LoggerBase {
		public ConsoleLogger() 
			: this(new LoggerSettings()) {
		}

		public ConsoleLogger(LoggerSettings settings) 
			: base(settings) {
		}

		protected override void LogMessage(string message) {
			Console.Out.WriteLine(message);
		}
	}
}
