using System;

using Deveel.Data.Configuration;
using Deveel.Diagnostics;

namespace Deveel.Diagnostics {
	public class ConsoleLogger : ILogger {
		public void Dispose() {
		}

		public void Init(IDbConfig config) {
		}

		public bool IsInterestedIn(LogLevel level) {
			return true;
		}

		public void Log(LogEntry entry) {
			Console.Out.WriteLine("[{0}] {1:s} ({2}) - {3}", entry.Level.Name, entry.Time, entry.Source, entry.Message);

			if (entry.HasError) {
				Console.Out.WriteLine(entry.Error.Message);
				Console.Out.WriteLine(entry.Error.StackTrace);
			}
		}
	}
}
