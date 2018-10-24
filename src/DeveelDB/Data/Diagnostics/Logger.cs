using System;
using System.Threading.Tasks;

namespace Deveel.Data.Diagnostics {
	public abstract class Logger : ILogger {
		static Logger() {
			Empty = new EmptyLogger();
		}

		public static ILogger Empty { get; }

		public abstract bool IsInterestedIn(LogLevel level);

		public abstract Task LogAsync(LogEntry entry);

		#region EmptyLogger

		class EmptyLogger : Logger {
			public override bool IsInterestedIn(LogLevel level) {
				return true;
			}

			public override Task LogAsync(LogEntry entry) {
				return Task.CompletedTask;
			}
		}

		#endregion
	}
}