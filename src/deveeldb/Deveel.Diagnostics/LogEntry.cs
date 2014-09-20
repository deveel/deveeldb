// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Diagnostics {
	/// <summary>
	/// The class <see cref="LogEntry"/> represents a new entry in a log.
	/// </summary>
	/// <remarks>
	/// This is only used in <see cref="DefaultLogger"/> to maintain
	/// information concerning a log entry.
	/// </remarks>
	public sealed class LogEntry {
		private readonly string thread;
		private readonly string message;
		private readonly string source;
		private readonly Exception error;
		private readonly LogLevel level;
		private readonly DateTime time;

		/// <summary>
		/// Internal constructor to avoid the use externally.
		/// </summary>
		/// <param name="thread"></param>
		/// <param name="level"></param>
		/// <param name="source"></param>
		/// <param name="message"></param>
		/// <param name="time"></param>
		internal LogEntry(string thread, LogLevel level, string source, string message, DateTime time) {
			this.thread = thread;
			this.message = message;
			this.time = time;
			this.level = level;
			this.source = source;
		}

		internal LogEntry(string thread, LogLevel level, string source, Exception error, DateTime time)
			: this(thread, level, source, error.Message, time) {
			this.error = error;
		}

		/// <summary>
		/// Gets ths identification of the current thread logging.
		/// </summary>
		public string Thread {
			get { return thread; }
		}

		/// <summary>
		/// Gets the error component of the entry, if any.
		/// </summary>
		public Exception Error {
			get { return error; }
		}

		/// <summary>
		/// Getrs a boolean flag indicating if the entry contains an error component.
		/// </summary>
		/// <seealso cref="Error"/>
		public bool HasError {
			get { return error != null; }
		}

		/// <summary>
		/// Gets the time of the event logged.
		/// </summary>
		public DateTime Time {
			get { return time; }
		}

		/// <summary>
		/// Gets the level of the logged entry.
		/// </summary>
		public LogLevel Level {
			get { return level; }
		}

		/// <summary>
		/// Gets the source of the logging.
		/// </summary>
		/// <remarks>
		/// Generally this is the name of the type executing a function
		/// and the method name.
		/// </remarks>
		public string Source {
			get { return source; }
		}

		/// <summary>
		/// Gets the message part of the log.
		/// </summary>
		public string Message {
			get { return message; }
		}
	}
}