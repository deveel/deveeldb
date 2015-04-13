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
			var errorEvent = logEntry.Event as ErrorEvent;
			if (errorEvent == null)
				return;

			lock (this) {
				try {
					var message = FormatMessage(logEntry, errorEvent);
					WriteToLog(message);
				} catch (Exception ex) {
				}
			}
		}

		protected virtual void WriteToLog(string message) {
			// TODO:
		}

		private string FormatMessage(EventLog logEntry, ErrorEvent errorEvent) {
			var format = LogFormat;
			format = format.Replace("[date]", FormatDate(logEntry.Date));
			format = format.Replace("[user]", logEntry.Event.UserName);
			format = format.Replace("[database]", logEntry.Event.DatabaseName);
			format = format.Replace("[level]", errorEvent.Level.ToString().ToUpperInvariant());
			format = format.Replace("[message]", errorEvent.Message);
			format = format.Replace("[code]", FormatErrorCode(errorEvent.EventClass, errorEvent.EventClass));
			format = format.Replace("[stack]", GetStackTrace(errorEvent.Data));
			format = format.Replace("[source]", GetSource(errorEvent.Data));
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

		private static string GetStackTrace(IDictionary<string, object> data) {
			object obj;
			if (data == null ||
			    !data.TryGetValue("StackTrace", out obj))
				return null;

			return (string) obj;
		}

		private static string GetSource(IDictionary<string, object> data) {
			object obj;
			if (data == null ||
				!data.TryGetValue("StackTrace", out obj))
				return null;

			return (string)obj;			
		}
	}
}
