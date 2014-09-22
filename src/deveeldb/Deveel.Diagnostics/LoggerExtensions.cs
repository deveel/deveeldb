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
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Threading;

using Deveel.Data.Configuration;

namespace Deveel.Diagnostics {
	public static class LoggerExtensions {
		private static string GetCallingMethod(int skipFrames) {
			StackFrame frame = new StackFrame(skipFrames + 1);
			MethodBase method = frame.GetMethod();
			return String.Format("{0}.{1}", method.ReflectedType.FullName, method.Name);
		}

		public static void Log(this ILogger logger, LogLevel level, string message) {
			Log(logger, level, GetCallingMethod(1), message);
		}

		public static void Log(this ILogger logger, LogLevel level, string source, string message) {
			Log(logger, level, source, message, null);
		}

		public static void LogFormat(this ILogger logger, LogLevel level, string format, params object[] args) {
			LogFormat(logger, level, GetCallingMethod(1), format, args);
		}

		public static void LogFormat(this ILogger logger, LogLevel level, string source, string format, params object[] args) {
			Log(logger, level, source, String.Format(format, args));
		}

		public static void Log(this ILogger logger, LogLevel level, string source, string message, Exception error) {
			Log(logger, level, source, message, error, DateTime.UtcNow);
		}

		public static void Log(this ILogger logger, LogLevel level, string source, string message, DateTime date) {
			Log(logger, level, source, message, null, date);
		}

		public static void Log(this ILogger logger, LogLevel level, string message, Exception error) {
			Log(logger, level, GetCallingMethod(1), message, error, DateTime.UtcNow);
		}

		public static void Log(this ILogger logger, LogLevel level, string message, Exception error, DateTime date) {
			Log(logger, level, GetCallingMethod(1), message, error, date);
		}

		public static void Log(this ILogger logger, LogLevel level, string source, string message, Exception error, DateTime date) {
			logger.Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(), level, source, message, error, date));
		}

		public static void Log(this ILogger logger, LogLevel level, object source, string message) {
			Log(logger, level, source, message, null);
		}

		public static void Log(this ILogger logger, LogLevel level, object source, string message, Exception error) {
			Log(logger, level, source, message, error, DateTime.UtcNow);
		}

		public static void Log(this ILogger logger, LogLevel level, object source, string message, DateTime date) {
			Log(logger, level, source, message, null, date);
		}

		public static void Log(this ILogger logger, LogLevel level, object source, string message, Exception error, DateTime date) {
			logger.Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(CultureInfo.InvariantCulture), level, source.ToString(), message, error, date));
		}

		public static void LogFormat(this ILogger logger, LogLevel level, object source, string format, params object[] args) {
			Log(logger, level, source, String.Format(format, args));
		}

		public static void Info(this ILogger logger, object source, string message) {
			Info(logger, source, message, null);
		}

		public static void Info(this ILogger logger, object source, Exception error) {
			Info(logger, source, null, error);
		}

		public static void Info(this ILogger logger, object source, string message, Exception error) {
			Info(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Info(this ILogger logger, object source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Info, source, message, error, date);
		}

		public static void InfoFormat(this ILogger logger, object source, string format, params object[] args) {
			logger.Log(LogLevel.Info, source, String.Format(format, args));
		}

		public static void Info(this ILogger logger, string source, string message) {
			Info(logger, source, message, null);
		}

		public static void Info(this ILogger logger, string source, string message, Exception error) {
			Info(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Info(this ILogger logger, string source, string message, DateTime date) {
			Info(logger, source, message, null, date);
		}

		public static void Info(this ILogger logger, string message) {
			Info(logger, GetCallingMethod(1), message, null);
		}

		public static void Info(this ILogger logger, string message, Exception error) {
			Info(logger, GetCallingMethod(1), message, error, DateTime.UtcNow);
		}

		public static void Info(this ILogger logger, string message, Exception error, DateTime date) {
			Info(logger, GetCallingMethod(1), message, error, date);
		}

		public static void Info(this ILogger logger, string source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Info, source, message, error, date);
		}

		public static void Error(this ILogger logger, string source, string message) {
			Error(logger, source, message, null);
		}

		public static void Error(this ILogger logger, string source, string message, Exception error) {
			Error(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Error(this ILogger logger, string source, string message, DateTime date) {
			Error(logger, source, message, null, date);
		}

		public static void Error(this ILogger logger, string source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Error, source, message, error, date);
		}

		public static void ErrorFormat(this ILogger logger, string format, params object[] args) {
			ErrorFormat(logger, GetCallingMethod(1), format, args);
		}

		public static void ErrorFormat(this ILogger logger, string source, string format, params object[] args) {
			logger.Log(LogLevel.Error, source, string.Format(format, args));
		}

		public static void Error(this ILogger logger, object source, string message) {
			Error(logger, source, message, null);
		}

		public static void Error(this ILogger logger, object source, string message, Exception error) {
			Error(logger, source, message, error, DateTime.UtcNow);
		}

		public static void ErrorFormat(this ILogger logger, object source, string format, params object[] args) {
			Error(logger, source, string.Format(format, args));
		}

		public static void Error(this ILogger logger, object source, string message, DateTime date) {
			Error(logger, source, message, null, date);
		}

		public static void Error(this ILogger logger, string message, Exception error) {
			Error(logger, GetCallingMethod(1), message, error, DateTime.UtcNow);
		}

		public static void Error(this ILogger logger, string message) {
			Error(logger, GetCallingMethod(1), message, DateTime.UtcNow);
		}

		public static void Error(this ILogger logger, string message, DateTime date) {
			Error(logger, GetCallingMethod(1), message, null, date);
		}

		public static void Error(this ILogger logger, Exception error) {
			Error(logger, GetCallingMethod(1), null, error, DateTime.UtcNow);
		}

		public static void Error(this ILogger logger, Exception error, DateTime date) {
			Error(logger, GetCallingMethod(1), null, error, date);
		}

		public static void Error(this ILogger logger, string message, Exception error, DateTime date) {
			Error(logger, GetCallingMethod(1), message, error, date);
		}

		public static void Error(this ILogger logger, object source, Exception error) {
			Error(logger, source, error, DateTime.UtcNow);
		}

		public static void Error(this ILogger logger, object source, Exception error, DateTime date) {
			Error(logger, source, null, error, date);
		}

		public static void Error(this ILogger logger, object source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Error, source, message, error, date);
		}

		public static void Debug(this ILogger logger, string source, string message) {
			Debug(logger, source, message, null);
		}

		public static void Debug(this ILogger logger, string source, string message, Exception error) {
			Debug(logger, source, message, error, DateTime.UtcNow);
		}

		public static void DebugFormat(this ILogger logger, string format, params object[] args) {
			DebugFormat(logger, GetCallingMethod(1), format, args);
		}

		public static void DebugFormat(this ILogger logger, string source, string format, params object[] args) {
			Debug(logger, source, String.Format(format, args));
		}

		public static void Debug(this ILogger logger, string source, Exception error) {
			Debug(logger, source, error, DateTime.UtcNow);
		}

		public static void Debug(this ILogger logger, string source, Exception error, DateTime date) {
			Debug(logger, source, null, error, date);
		}

		public static void Debug(this ILogger logger, string source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Debug, source, message, error, date);
		}

		public static void Debug(this ILogger logger, object source, string message) {
			Debug(logger, source, message, null);
		}

		public static void DebugFormat(this ILogger logger, object source, string format, params object[] args) {
			Debug(logger, source, String.Format(format, args));
		}

		public static void Debug(this ILogger logger, object source, string message, Exception error) {
			Debug(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Debug(this ILogger logger, object source, string message, DateTime date) {
			Debug(logger, source, message, null, date);
		}

		public static void Debug(this ILogger logger, object source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Debug, source, message, error, date);
		}

		public static void Warning(this ILogger logger, object source, string message) {
			Warning(logger, source, message, null);
		}

		public static void WarningFormat(this ILogger logger, string format, params object[] args) {
			Warning(logger, GetCallingMethod(1), string.Format(format, args));
		}

		public static void WarningFormat(this ILogger logger, object source, string format, params object[] args) {
			Warning(logger, source, string.Format(format, args));
		}

		public static void Warning(this ILogger logger, object source, Exception error) {
			Warning(logger, source, null, error);
		}

		public static void Warning(this ILogger logger, object source, string message, Exception error) {
			Warning(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Warning(this ILogger logger, object source, string message, DateTime date) {
			Warning(logger, source, message, null, date);
		}

		public static void Warning(this ILogger logger, object source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Warning, source, message, error, date);
		}

		public static void Warning(this ILogger logger, string source, string message) {
			Warning(logger, source, message, null);
		}

		public static void Warning(this ILogger logger, string source, string message, Exception error) {
			Warning(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Warning(this ILogger logger, string source, string message, DateTime date) {
			Warning(logger, source, message, null, date);
		}

		public static void Warning(this ILogger logger, string source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Warning, source, message, error, date);
		}

		public static void Warning(this ILogger logger, string message, Exception error) {
			Warning(logger, message, error, DateTime.UtcNow);
		}

		public static void Warning(this ILogger logger, string message) {
			Warning(logger, GetCallingMethod(1), message, DateTime.UtcNow);
		}

		public static void Warning(this ILogger logger, string message, DateTime date) {
			Warning(logger, GetCallingMethod(1), message, null, date);
		}

		public static void Warning(this ILogger logger, Exception error) {
			Warning(logger, GetCallingMethod(1), error, DateTime.UtcNow);
		}

		public static void Warning(this ILogger logger, Exception error, DateTime date) {
			Warning(logger, GetCallingMethod(1), null, error, date);
		}

		public static void Warning(this ILogger logger, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Warning, GetCallingMethod(1), message, error, date);
		}

		public static void Trace(this ILogger logger, string source, string message) {
			Trace(logger, source, message, null);
		}

		public static void Trace(this ILogger logger, string source, string message, Exception error) {
			Trace(logger, source, message, error, DateTime.UtcNow);
		}

		public static void TraceFormat(this ILogger logger, string format, params object[] args) {
			TraceFormat(logger, GetCallingMethod(1), format, args);
		}

		public static void TraceFormat(this ILogger logger, string source, string format, params object[] args) {
			Trace(logger, source, String.Format(format, args));
		}

		public static void Trace(this ILogger logger, string source, string message, DateTime date) {
			Trace(logger, source, message, null, date);
		}

		public static void Trace(this ILogger logger, string source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Trace, source, message, error, date);
		}

		public static void Trace(this ILogger logger, object source, string message) {
			Trace(logger, source, message, null);
		}

		public static void TraceFormat(this ILogger logger, object source, string format, params object[] args) {
			Trace(logger, source, String.Format(format, args));
		}

		public static void Trace(this ILogger logger, object source, string message, Exception error) {
			Trace(logger, source, message, error, DateTime.UtcNow);
		}

		public static void Trace(this ILogger logger, object source, string message, DateTime date) {
			Trace(logger, source, message, null, date);
		}

		public static void Trace(this ILogger logger, object source, Exception error) {
			Trace(logger, source, error, DateTime.UtcNow);
		}

		public static void Trace(this ILogger logger, object source, Exception error, DateTime date) {
			Trace(logger, source, null, error, date);
		}

		public static void Trace(this ILogger logger, object source, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Trace, source, message, error, date);
		}

		public static void Trace(this ILogger logger, string message, Exception error) {
			Trace(logger, GetCallingMethod(1), message, error);
		}

		public static void Trace(this ILogger logger, string message) {
			Trace(logger, GetCallingMethod(1), message, DateTime.UtcNow);
		}

		public static void Trace(this ILogger logger, string message, DateTime date) {
			Trace(logger, GetCallingMethod(1), message, date);
		}

		public static void Trace(this ILogger logger, string message, Exception error, DateTime date) {
			logger.Log(LogLevel.Trace, GetCallingMethod(1), message, error, date);
		}
	}
}