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
using System.Collections.Generic;
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	public class ErrorLogger : IEventLogger {
		public const string DefaultDateFormat = "yyyy-MM-ddTHH:mm:ss";

		public ErrorLevel MinimumLevel { get; set; }

		public ErrorLevel MaximumLevel { get; set; }

		public string LogFormat { get; set; }

		public string DateFormat { get; set; }

		public void SetLevel(ErrorLevel level) {
			MinimumLevel = level;
			MaximumLevel = level;
		}

		bool IEventLogger.CanLog(LogLevel level) {
			var errorLevel = (ErrorLevel) level;
			return CanLog(errorLevel);
		}

		public bool CanLog(ErrorLevel level) {
			return level <= MaximumLevel || level >= MinimumLevel;
		}

		public void LogEvent(EventLog logEntry) {
			var errorEvent = logEntry.Event;
			if (errorEvent == null)
				return;

			lock (this) {
				try {
					var message = FormatMessage(logEntry, errorEvent);
					WriteToLog(message);
				} catch (Exception) {
				}
			}
		}

		protected virtual void WriteToLog(string message) {
			// TODO:
		}

		private string FormatMessage(EventLog logEntry, IEvent errorEvent) {
			var errorLevel = errorEvent.ErrorLevel().ToString().ToUpperInvariant();

			var format = LogFormat;
			format = format.Replace("[date]", FormatDate(logEntry.Date));
			format = format.Replace("[user]", logEntry.Event.UserName());
			format = format.Replace("[database]", logEntry.Event.Database());
			format = format.Replace("[level]", errorLevel);
			format = format.Replace("[message]", errorEvent.EventMessage);
			format = format.Replace("[code]", FormatErrorCode(errorEvent.EventClass, errorEvent.EventClass));
			format = format.Replace("[stack]", errorEvent.StackTrace());
			format = format.Replace("[source]", errorEvent.ErrorSource());
			return format;
		}

		private string FormatDate(DateTime date) {
			var format = DateFormat;
			if (String.IsNullOrEmpty(format))
				format = DefaultDateFormat;

			return date.ToString(format, CultureInfo.InvariantCulture);
		}

		private static string FormatErrorCode(int errorClass, int errorCode) {
			return String.Format("{0:X}:{1:X}", errorClass, errorCode);
		}
	}
}
