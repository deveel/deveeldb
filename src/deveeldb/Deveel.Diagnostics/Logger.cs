// 
//  Copyright 2011 Deveel
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
using System.Diagnostics;
using System.Reflection;
using System.Threading;

using Deveel.Data.Control;

namespace Deveel.Diagnostics {
	/// <summary>
	/// An <seealso cref="ILogger"/> instance that abstracts all the
	/// needed logging mechanisms required by the system to make it
	/// coherent and reduce the implementation-specific dependancies,
	/// wrpping the concrete implementation of the logger to pass-through.
	/// </summary>
	public sealed class Logger : ILogger {
		private ILogger logger;

		internal Logger(ILogger logger) {
			this.logger = logger;
		}

		~Logger() {
			Dispose(false);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (logger != null)
					logger.Dispose();
				logger = null;
			}
		}

		private string GetCallingMethod(int skipFrames) {
			StackFrame frame = new StackFrame(skipFrames + 1);
			MethodBase method = frame.GetMethod();
			return String.Format("{0}.{1}", method.ReflectedType.FullName, method.Name);			
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		void ILogger.Init(DbConfig config) {
			// this logger cannot be configured...
		}

		public bool IsInterestedIn(LogLevel level) {
			return logger.IsInterestedIn(level);
		}

		public void Log(LogEntry entry) {
			logger.Log(entry);
		}

		public void Log(LogLevel level, string message) {
			string methodName = GetCallingMethod(1);
			Log(level, methodName, message);
		}

		public void Log(LogLevel level, Exception error) {
			string methodName = GetCallingMethod(1);
			Log(level, methodName, error);
		}

		public void Log(LogLevel level, string source, string message) {
			Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(), level, source, message, DateTime.Now));
		}

		public void Log(LogLevel level, string source, Exception error) {
			Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(), level, source, error, DateTime.Now));
		}

		public void Log(LogLevel level, object source, Exception error) {
			Log(level, source.ToString(), error);
		}

		public void Log(LogLevel level, object source, string message) {
			Log(level, source.ToString(), message);
		}

		public void Error(string source, string message) {
			Log(LogLevel.Error, source, message);
		}

		public void Error(object source, string message) {
			Log(LogLevel.Error, source, message);
		}

		public void Error(string source, Exception error) {
			Log(LogLevel.Error, source, error);
		}

		public void Error(object source, Exception error) {
			Log(LogLevel.Error, source, error);
		}

		public void Error(Exception error) {
			string methodName = GetCallingMethod(1);
			Error(methodName, error);
		}

		public  void Info(string source, string message) {
			Log(LogLevel.Information, source, message);
		}

		public void Info(object source, string message) {
			Log(LogLevel.Information, source, message);
		}

		public void Info(string source, Exception error) {
			Log(LogLevel.Information, source, error);
		}

		public void Info(object source, Exception error) {
			Log(LogLevel.Information, source, error);
		}

		public void Warning(object source, string message) {
			Log(LogLevel.Warning, source, message);
		}

		public void Warning(string source, string message) {
			Log(LogLevel.Warning, source, message);
		}

		public void Warning(object source, Exception error) {
			Log(LogLevel.Warning, source, error);
		}

		public void Warning(string source, Exception error) {
			Log(LogLevel.Warning, source, error);
		}

		public void Message(object source, string message) {
			Log(LogLevel.Message, source, message);
		}

		public void Message(string source, string message) {
			Log(LogLevel.Message, source, message);
		}

		public void Message(object source, Exception error) {
			Log(LogLevel.Message, source, error);
		}

		public void Message(string source, Exception error) {
			Log(LogLevel.Message, source, error);
		}
	}
}
