using System;

namespace Deveel.Data.Diagnostics {
	public interface IEventLogger {
		bool CanLog(LogLevel level);

		void LogEvent(EventLog logEntry);
	}
}
