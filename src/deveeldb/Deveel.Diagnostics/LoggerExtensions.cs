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
using System.Reflection;
using System.Threading;

namespace Deveel.Diagnostics {
	public static class LoggerExtensions {
		private static string GetCallingMethod(int skipFrames) {
			StackFrame frame = new StackFrame(skipFrames + 1);
			MethodBase method = frame.GetMethod();
			return String.Format("{0}.{1}", method.ReflectedType.FullName, method.Name);
		}

		public static void Log(this ILogger logger, LogLevel level, string message) {
			string methodName = GetCallingMethod(1);
			logger.Log(level, methodName, message);
		}

		public static void Log(this ILogger logger, LogLevel level, Exception error) {
			string methodName = GetCallingMethod(1);
			logger.Log(level, methodName, error);
		}

		public static void Log(this ILogger logger, LogLevel level, string source, string message) {
			logger.Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(), level, source, message, DateTime.Now));
		}

		public static void Log(this ILogger logger, LogLevel level, string source, Exception error) {
			logger.Log(new LogEntry(Thread.CurrentThread.ManagedThreadId.ToString(), level, source, error, DateTime.Now));
		}

		public static void Log(this ILogger logger, LogLevel level, object source, Exception error) {
			logger.Log(level, source.ToString(), error);
		}

		public static void Log(this ILogger logger, LogLevel level, object source, string message) {
			logger.Log(level, source.ToString(), message);
		}

		public static void Error(this ILogger logger, string source, string message) {
			logger.Log(LogLevel.Error, source, message);
		}

		public static void Error(this ILogger logger, object source, string message) {
			logger.Log(LogLevel.Error, source, message);
		}

		public static void Error(this ILogger logger, string source, Exception error) {
			logger.Log(LogLevel.Error, source, error);
		}

		public static void Error(this ILogger logger, object source, Exception error) {
			logger.Log(LogLevel.Error, source, error);
		}

		public static void Error(this ILogger logger, Exception error) {
			string methodName = GetCallingMethod(1);
			logger.Error(methodName, error);
		}

		public static void Info(this ILogger logger, string source, string message) {
			logger.Log(LogLevel.Info, source, message);
		}

		public static void Info(this ILogger logger, object source, string message) {
			logger.Log(LogLevel.Info, source, message);
		}

		public static void Info(this ILogger logger, string source, Exception error) {
			logger.Log(LogLevel.Info, source, error);
		}

		public static void Info(this ILogger logger, object source, Exception error) {
			logger.Log(LogLevel.Info, source, error);
		}

		public static void Warning(this ILogger logger, object source, string message) {
			logger.Log(LogLevel.Warning, source, message);
		}

		public static void Warning(this ILogger logger, string source, string message) {
			logger.Log(LogLevel.Warning, source, message);
		}

		public static void Warning(this ILogger logger, object source, Exception error) {
			logger.Log(LogLevel.Warning, source, error);
		}

		public static void Warning(this ILogger logger, string source, Exception error) {
			logger.Log(LogLevel.Warning, source, error);
		}

		public static void Message(this ILogger logger, object source, string message) {
			logger.Log(LogLevel.Message, source, message);
		}

		public static void Message(this ILogger logger, string source, string message) {
			logger.Log(LogLevel.Message, source, message);
		}

		public static void Message(this ILogger logger, object source, Exception error) {
			logger.Log(LogLevel.Message, source, error);
		}

		public static void Message(this ILogger logger, string source, Exception error) {
			logger.Log(LogLevel.Message, source, error);
		}

		public static void Query(this ILogger logger, string source, string message) {
			logger.Log(LogLevel.Query, source, message);
		}

		public static void Query(this ILogger logger, object source, string message) {
			logger.Log(LogLevel.Query, source, message);
		}
	}
}