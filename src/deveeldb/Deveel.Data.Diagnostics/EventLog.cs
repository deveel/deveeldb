using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// An object that encapsulates all the meta information
	/// concerning an event to log.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This object is constructed from <see cref="LogEventRouter"/> class
	/// to be dispatched to all implementations of <see cref="IEventLogger"/>
	/// configured within the system.
	/// </para>
	/// </remarks>
	public sealed class EventLog {
		internal EventLog(int eventClass, int eventCode, object source, LogLevel level, string database, string userName, string remoteAddress, string message, Exception exception) {
			EventClass = eventClass;
			EventCode = eventCode;
			Source = source;
			Level = level;
			Database = database;
			UserName = userName;
			RemoteAddress = remoteAddress;
			Message = message;
			Exception = exception;

			// TODO: Get this from the event?
			TimeStamp = DateTime.UtcNow;
		}

		/// <summary>
		/// Gets an optional user-defined message that explains the event.
		/// </summary>
		public string Message { get; private set; }

		/// <summary>
		/// Gets an optional <see cref="Exception"/> that was thrown by the system.
		/// </summary>
		public Exception Exception { get; private set; }

		/// <summary>
		/// Gets the time-stamp of the event.
		/// </summary>
		public DateTime TimeStamp { get; private set; }

		/// <summary>
		/// Gets the level of the event to be logged.
		/// </summary>
		public LogLevel Level { get; private set; }

		/// <summary>
		/// Gets the name of the database where this event happened.
		/// </summary>
		public string Database { get; private set; }

		/// <summary>
		/// Gets the optional name of the user that caused the event.
		/// </summary>
		public string UserName { get; private set; }

		/// <summary>
		/// In case of remote connection, gets the string that defines the
		/// address of user endpoint.
		/// </summary>
		public string RemoteAddress { get; private set; }

		/// <summary>
		/// Gets the numeric code that represents the class of the event.
		/// </summary>
		public int EventClass { get; private set; }

		/// <summary>
		/// Gets the code of the event within the class specified.
		/// </summary>
		public int EventCode { get; private set; }

		/// <summary>
		/// Gets the source of the event.
		/// </summary>
		public object Source { get; private set; }
	}
}
