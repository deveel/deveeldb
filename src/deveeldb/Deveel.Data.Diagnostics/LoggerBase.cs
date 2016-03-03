// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
