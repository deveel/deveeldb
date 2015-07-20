using System;

namespace Deveel.Data.Diagnostics {
	public sealed class EventLog {
		public string Message { get; set; }

		public Exception Exception { get; set; }

		public DateTime TimeStamp { get; set; }

		public LogLevel Level { get; set; }

		public string Database { get; set; }

		public string UserName { get; set; }

		public string RemoteAddress { get; set; }

		public int EventClass { get; set; }

		public int EventCode { get; set; }

		public object Source { get; set; }
	}
}
