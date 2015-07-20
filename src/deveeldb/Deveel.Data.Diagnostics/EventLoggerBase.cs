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

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// A base class for simple event loggers provided from the system
	/// </summary>
	/// <remarks>
	/// <para>
	/// This implementation of <see cref="IEventLogger"/> and the inheriting
	/// classes are trivial implementations, to provide with a minimal
	/// logging functionality, not to involve third parties logging
	/// middle-wares.
	/// </para>
	/// </remarks>
	public abstract class EventLoggerBase : IEventLogger, IConfigurable {
		/// <summary>
		/// The default date-time format used if none if configured.
		/// </summary>
		public const string DefaultDateFormat = "yyyy-MM-ddTHH:mm:ss";

		/// <summary>
		/// Gets or sets the minimum level to listen to.
		/// </summary>
		public LogLevel MinimumLevel { get; set; }

		/// <summary>
		/// Gets or sets the maximum level to listen to.
		/// </summary>
		public LogLevel MaximumLevel { get; set; }

		/// <summary>
		/// Gets or sets the format of the output message.
		/// </summary>
		public string LogFormat { get; set; }

		/// <summary>
		/// Gets or sets the format to use when converting a date-time
		/// to a string.
		/// </summary>
		/// <remarks>
		/// If nothing is configured for this property, the <see cref="DefaultDateFormat"/>
		/// value is used.
		/// </remarks>
		public string DateFormat { get; set; }

		/// <summary>
		/// Sets the exact logging level to listen to.
		/// </summary>
		/// <param name="level">The log level to listen to.</param>
		public void SetLevel(LogLevel level) {
			MinimumLevel = level;
			MaximumLevel = level;
		}

		public bool CanLog(LogLevel level) {
			return level <= MaximumLevel || level >= MinimumLevel;
		}

		public void LogEvent(EventLog logEntry) {
			lock (this) {
				try {
					var message = FormatMessage(logEntry);
					WriteToLog(message);
				} catch (Exception) {
					// At this point we totally ignore any exception
				}
			}
		}

		protected abstract void WriteToLog(string message);

		private string FormatMessage(EventLog logEntry) {
			var format = LogFormat;
			format = format.Replace("[date]", FormatDate(logEntry.TimeStamp));
			format = format.Replace("[user]", logEntry.UserName);
			format = format.Replace("[database]", logEntry.Database);
			format = format.Replace("[level]", logEntry.Level.ToString().ToUpperInvariant());
			format = format.Replace("[message]", logEntry.Message);
			format = format.Replace("[code]", FormatErrorCode(logEntry.EventClass, logEntry.EventCode));
			format = format.Replace("[source]", logEntry.Source.ToString());
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

		void IConfigurable.Configure(IDbConfig config) {
			OnConfigure(config);
		}

		protected virtual void OnConfigure(IDbConfig config) {
		}
	}
}
