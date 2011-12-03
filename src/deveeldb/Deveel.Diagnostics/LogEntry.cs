// 
//  Copyright 2010  Deveel
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
using System.Text;

namespace Deveel.Diagnostics {
	/// <summary>
	/// The class <see cref="LogEntry"/> represents a new entry in a log.
	/// </summary>
	/// <remarks>
	/// This is only used in <see cref="DefaultDebugLogger"/> to maintain
	/// information concerning a log entry.
	/// </remarks>
	public sealed class LogEntry {
		private readonly string thread;
		private readonly string message;
		private readonly string source;
		private readonly DebugLevel level;
		private readonly DateTime time;

		/// <summary>
		/// Internal constructor to avoid the use externally.
		/// </summary>
		/// <param name="thread"></param>
		/// <param name="message"></param>
		/// <param name="source"></param>
		/// <param name="level"></param>
		/// <param name="time"></param>
		internal LogEntry(string thread, string message, string source, DebugLevel level, DateTime time) {
			this.thread = thread;
			this.message = message;
			this.time = time;
			this.level = level;
			this.source = source;
		}

		/// <summary>
		/// Gets ths identification of the current thread logging.
		/// </summary>
		public string Thread {
			get { return thread; }
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
		public DebugLevel Level {
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

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			sb.Append("Thread: ");
			sb.Append(thread);
			sb.Append(", ");
			sb.Append("Time: ");
			sb.Append(time);
			sb.Append(", ");
			sb.Append("Level: ");
			sb.Append(level);
			sb.Append(", ");
			sb.Append("Source: ");
			sb.Append(source);
			sb.Append(", ");
			sb.Append("Message: ");
			if (message.Length > 60) {
				sb.Append(message.Substring(0, 57));
				sb.Append("...");
			} else {
				sb.Append(message);
			}

			return sb.ToString();
		}
	}
}