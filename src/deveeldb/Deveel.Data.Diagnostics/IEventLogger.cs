using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Defines a contract for event loggers.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface have the responsibility
	/// to log events fired from the system into a configured medium.
	/// </para>
	/// </remarks>
	public interface IEventLogger {
		/// <summary>
		/// Logs the given entry to the medium of the logger.
		/// </summary>
		/// <param name="entry">The information to be logged.</param>
		void LogEvent(EventLog entry);

		/// <summary>
		/// Verifies if the logger is listening to the given level of logs.
		/// </summary>
		/// <param name="level">The level of information to log.</param>
		/// <returns>
		/// Returns <c>true</c> if the logger is configured to listen to
		/// the specified level of information, otherwise it returns <c>false</c>.
		/// </returns>
		bool CanLog(LogLevel level);
	}
}
