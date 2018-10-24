using System;
using System.Threading.Tasks;

namespace Deveel.Data.Diagnostics {
	public interface ILogger {
		bool IsInterestedIn(LogLevel level);

		Task LogAsync(LogEntry entry);
	}
}