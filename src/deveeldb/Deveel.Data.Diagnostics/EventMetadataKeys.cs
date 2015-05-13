using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// The default metadata keys that can be set into the
	/// <see cref="IEvent.EventData"/> container.
	/// </summary>
	public static class EventMetadataKeys {
		/// <summary>
		/// The key for the database name event data.
		/// </summary>
		public const string Database = "Database";

		/// <summary>
		/// The key for the user name who originated the event.
		/// </summary>
		public const string UserName = "UserName";

		/// <summary>
		/// The key of the remote address of a connection.
		/// </summary>
		public const string RemoteAddress = "Remote-Address";

		/// <summary>
		/// The key of the connection protocol name of a session that
		/// originated the event.
		/// </summary>
		public const string Protocol = "Protocol";

		/// <summary>
		/// The key for the error stack trace in an error event.
		/// </summary>
		public const string StackTrace = "StackTrace";

		/// <summary>
		/// The key for the error source in an error event.
		/// </summary>
		public const string Source = "Source";

		/// <summary>
		/// The level of an error event.
		/// </summary>
		public const string ErrorLevel = "Error-Level";
	}
}
