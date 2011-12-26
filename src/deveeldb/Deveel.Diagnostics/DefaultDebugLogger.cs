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
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;

using Deveel.Data.Control;

namespace Deveel.Diagnostics {
	/// <summary>
	/// A default implementation of <see cref="IDebugLogger"/> that logs 
	/// messages to a <see cref="TextWriter"/> object.
	/// </summary>
	/// <remarks>
	/// This implementation allows for filtering of log messages of particular
	/// depth.  So for example, only message above or equal to level Alert are
	/// shown.
	/// </remarks>
	public class DefaultDebugLogger : IDebugLogger {
		/// <summary>
		/// The debug Lock object.
		/// </summary>
		private readonly Object debugLock = new Object();

		/// <summary>
		/// This variable specifies the level of debugging information that is
		/// output.  Any debugging output above this level is output.
		/// </summary>
		private int debugLevel;

		/// <summary>
		/// The string used to format the output message.
		/// </summary>
		private string messageFormat = "[{Thread}][{Time}] {Level:Name} - {Source} : {Message}";

		/// <summary>
		/// The text writer where the debugging information is output to.
		/// </summary>
		private TextWriter output;

		private bool disposed;

		~DefaultDebugLogger() {
			Dispose(false);
		}

		private void Write(LogEntry entry) {
			lock (debugLock) {
				if (output == null)
					return;

				StringBuilder sb = new StringBuilder();

				if (entry.Level < DebugLevel.Message) {
					sb.Append("> ");
				} else {
					sb.Append("% ");
				}

				sb.Append(FormatEntry(entry));
				output.WriteLine(sb.ToString());
				output.Flush();
			}
		}

		protected virtual string FormatEntry(LogEntry entry) {
			if (messageFormat == null || entry == null)
				return String.Empty;

			StringBuilder message = new StringBuilder();
			StringBuilder field = null;

			for (int i = 0; i < messageFormat.Length; i++) {
				char c = messageFormat[i];
				if (c == '{') {
					field = new StringBuilder();
					continue;
				} 
				if (c == '}' && field != null) {
					string fieldValue = field.ToString();
					string result = FormatField(entry, fieldValue);
					message.Append(result);
					field = null;
					continue;
				}

				if (field != null) {
					field.Append(c);
				} else {
					message.Append(c);
				}
			}

			return message.ToString();
		}

		protected virtual string FormatField(LogEntry entry, string fieldName) {
			if (String.IsNullOrEmpty(fieldName))
				return null;

			string format = null;
			int index = fieldName.IndexOf(':');
			if (index != -1) {
				format = fieldName.Substring(index + 1);
				fieldName = fieldName.Substring(0, index);
			}

			if (fieldName.Length == 0)
				return null;

			switch (fieldName.ToLower()) {
				case "message":
					return (format != null ? String.Format(format, entry.Message) : entry.Message);
				case "time":
					return (format != null ? entry.Time.ToString(format, CultureInfo.InvariantCulture) : entry.Time.ToString());
				case "source":
					return (format != null ? String.Format(format, entry.Source) : entry.Source);
				case "level": {
					if (format != null)
						format = format.ToLower();
					if (format == "number" || format == null)
						return entry.Level.Value.ToString();
					if (format == "name")
						return entry.Level.Name;
					return String.Format(format, entry.Level.Value);
				}
				case "thread": {
					string threadName = (entry.Thread ?? Thread.CurrentThread.ManagedThreadId.ToString());
					return (format != null ? String.Format(format, threadName) : threadName);
				}
				default:
					throw new ArgumentException("Unknown field " + fieldName);
			}
		}

		// ---------- Implemented from IDebugLogger ----------

		public void Init(DbConfig config) {
			string logPathString = config.LogPath;
			string rootPathVar = config.GetValue("root_path");
			bool readOnly = config.GetBooleanValue(ConfigKeys.ReadOnly, false);
			bool debugLogs = config.GetBooleanValue(ConfigKeys.DebugLogs, true);

			// Conditions for not initializing a log directory;
			//  1. Read only access is enabled
			//  2. log_path is empty or not set

			if (debugLogs && !readOnly && !String.IsNullOrEmpty(logPathString)) {
				// First set up the debug information in this VM for the 'Debug' class.
				string logPath = config.ParseFileString(rootPathVar, logPathString);
				// If the path doesn't exist the make it.
				if (!Directory.Exists(logPath))
					Directory.CreateDirectory(logPath);

				LogWriter fileWriter;
				string dlogFileName = config.DebugLogFile;
				string debugLogFile = Path.Combine(Path.GetFullPath(logPath), dlogFileName);

				try {
					// Allow log size to grow to 512k and allow 12 archives of the log
					//TODO: make it configurable...
					fileWriter = new LogWriter(debugLogFile, 512 * 1024, 12);
					fileWriter.WriteLine("**** Debug log started: {0} ****", DateTime.Now);
					fileWriter.Flush();
				} catch (IOException) {
					throw new Exception("Unable to open debug file '" + debugLogFile + "' in path '" + logPath + "'");
				}
				output = fileWriter;
			}

			// If 'debug_logs=disabled', don't Write out any debug logs
			if (!debugLogs) {
				// Otherwise set it up so the output from the logs goes to a TextWriter
				// that doesn't do anything.  Basically - this means all log information
				// will get sent into a black hole.
				output = new EmptyTextWriter();
			}

			debugLevel = config.GetIntegerValue(ConfigKeys.DebugLevel, -1);
			if (debugLevel == -1)
				// stops all the output
				debugLevel = 255;

			string format = config.GetValue("debug_format");
			if (format != null)
				messageFormat = format;
		}

		public bool IsInterestedIn(DebugLevel level) {
			return (level >= debugLevel);
		}

		public void Write(DebugLevel level, object ob, string message) {
			Write(level, ob.GetType().Name, message);
		}

		public void Write(DebugLevel level, Type type, string message) {
			Write(level, type.FullName, message);
		}

		public void Write(DebugLevel level, string type_string, string message) {
			if (IsInterestedIn(level)) {
				// InternalWrite(output, level, type_string, message);
				Thread thread = Thread.CurrentThread;
				Write(new LogEntry(thread.Name, message, type_string, level, DateTime.Now));
			}
		}

		/*
		private void WriteTime() {
			lock (output) {
				output.Write("[ TIME: ");
				output.Write(DateTime.Now.ToString());
				output.WriteLine(" ]");
				output.Flush();
			}
		}
		*/

		public void WriteException(Exception e) {
			WriteException(DebugLevel.Error, e);
		}

		public void WriteException(DebugLevel level, Exception e) {
			lock (this) {
				StringBuilder sb = new StringBuilder();
				sb.AppendLine(e.Message);
				sb.Append(e.StackTrace);
				Write(new LogEntry(Thread.CurrentThread.Name, sb.ToString(), null, level, DateTime.Now));
				/*
				if (IsInterestedIn(level)) {
					// we keep this way for exceptions, but we need to change it...
					lock (output) {
						WriteTime();
						output.Write("% ");
						output.WriteLine(e.Message);
						output.WriteLine(e.StackTrace);
						output.Flush();
					}
				}
				*/
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (output != null) {
						output.Close();
						output.Dispose();
					}
				}

				disposed = true;
			}
		}

		private class EmptyTextWriter : TextWriter {
			public override void Write(int c) {
			}

			public override void Write(char[] cbuf, int off, int len) {
			}

			public override void Flush() {
			}

			public override void Close() {
			}

			#region Overrides of TextWriter

			public override Encoding Encoding {
				get { return Encoding.ASCII; }
			}

			#endregion
		}
	}
}