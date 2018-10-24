using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public sealed class LogEntry {
		public LogEntry() {
			Level = LogLevel.Information;
			Data = new Dictionary<string, object>();
		}

		public string Message { get; set; }

		public LogLevel Level { get; set; }

		public IDictionary<string, object> Data { get; set; }
	}
}