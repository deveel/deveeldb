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
using System.Threading;
#if PCL
using System.Threading.Tasks;
#endif

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
	public abstract class EventLoggerBase : IEventLogger, IConfigurable, IDisposable {
#if PCL
		private Task writeThread;
#else
		private Thread writeThread;
#endif
		private Queue<EventLog> logs;
		private bool isRunning;

		/// <summary>
		/// The default date-time format used if none if configured.
		/// </summary>
		public const string DefaultDateFormat = "yyyy-MM-ddTHH:mm:ss";

		/// <summary>
		/// The default format of an output message for the loggers
		/// </summary>
		public const string DefaultMessageFormat = "[{level}][{date}] - {user}@{database}({source}) - {message}";

		protected EventLoggerBase() {
			LogFormat = DefaultMessageFormat;
			DateFormat = DefaultDateFormat;

			isRunning = true;
			logs = new Queue<EventLog>();

#if !PCL
			writeThread = new Thread(Write);
			writeThread.IsBackground = true;
			writeThread.Start();
#else
			writeThread = Task.Factory.StartNew(Write);
#endif
		}

		~EventLoggerBase() {
			Dispose(false);
		}

		/// <summary>
		/// Gets or sets the minimum level of log events to listen to
		/// </summary>
		public LogLevel Level { get; set; }

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

		public bool CanLog(LogLevel level) {
			return level >= Level;
		}

		private void Write() {
			while (isRunning) {
				if (logs == null)
					break;

				if (logs.Count == 0)
					Thread.Sleep(300);

				if (logs.Count > 0) {
					var log = logs.Dequeue();
					if (log == null)
						continue;

					var message = FormatMessage(log);
					WriteToLog(message);
				}
			}
		}

		public void LogEvent(EventLog logEntry) {
			//lock (this) {
			//	try {
			//		var message = FormatMessage(logEntry);
			//		WriteToLog(message);
			//	} catch (Exception) {
			//		// At this point we totally ignore any exception
			//	}
			//}

			logs.Enqueue(logEntry);
		}

		protected abstract void WriteToLog(string message);

		private string FormatMessage(EventLog logEntry) {
			var format = LogFormat;
			format = format.Replace("{date}", FormatDate(logEntry.TimeStamp));
			format = format.Replace("{user}", logEntry.UserName);
			format = format.Replace("{database}", logEntry.Database);
			format = format.Replace("{level}", logEntry.Level.ToString().ToUpperInvariant());
			format = format.Replace("{message}", logEntry.Message);
			format = format.Replace("{code}", FormatErrorCode(logEntry.EventClass, logEntry.EventCode));
			format = format.Replace("{source}", logEntry.Source != null ? logEntry.Source.ToString() : "NO SOURCE");
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

		void IConfigurable.Configure(IConfiguration config) {
			try {
				OnConfigure(config);
			} finally {
				configured = true;
			}
		}

		private bool configured;

		bool IConfigurable.IsConfigured {
			get { return configured; }
		}

		protected virtual void OnConfigure(IConfiguration config) {
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				isRunning = false;

				if (writeThread != null) {
#if PCL
					writeThread.Wait(5000);
#else
					try {
						// wait 5s for the writing to finish
						writeThread.Join(5000);
						writeThread.Abort();
					} catch (ThreadAbortException) {
					}
#endif
				}

				if (logs != null)
					logs.Clear();
			}

			logs = null;
			writeThread = null;
		}
	}
}
