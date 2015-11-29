using System;
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	public abstract class LoggerBase : ThreadedQueue<LogEntry> {
		protected LoggerBase(LoggerSettings settings) {
			Settings = settings;
		}

		~LoggerBase() {
			Dispose(false);
		}

		public LoggerSettings Settings { get; private set; }

		protected override void Consume(LogEntry message) {
			var logMessage = FormatMessage(message);
			LogMessage(logMessage);
		}

		private string FormatMessage(LogEntry entry) {
			var format = Settings.MessageFormat;
			foreach (var key in entry.Keys) {
				var value = FormatValue(entry.GetValue(key));
				var holder = String.Format("{0}", key);

				format = format.Replace(holder, value);
			}

			return format;
		}

		private string FormatValue(object value) {
			if (value == null)
				return String.Empty;

			if (value is string)
				return (string) value;
			if (value is DateTime ||
				value is DateTimeOffset) {
				if (String.IsNullOrEmpty(Settings.DateFormat))
					return value.ToString();

				if (value is DateTimeOffset)
					return ((DateTimeOffset) value).ToString(Settings.DateFormat);

				return ((DateTime) value).ToString(Settings.DateFormat);
			}

			if (!(value is IConvertible))
				return String.Empty;

			return (string) Convert.ChangeType(value, typeof (string), CultureInfo.InvariantCulture);
		}

		protected abstract void LogMessage(string message);
	}
}
