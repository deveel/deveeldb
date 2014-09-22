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
	///<summary>
	/// Debug level static values.
	///</summary>
	public sealed class LogLevel {
		private const int TraceLevelValue = 10000;
		private const int DebugLevelValue = 8000;
		private const int InfoLevelValue = 600;
		private const int ErrorLevelValue = 100;
		private const int WarningLevelValue = 20;

		private LogLevel(string name, int value) {
			Name = name;
			Value = value;
		}

		///<summary>
		/// General processing 'noise'
		///</summary>
		public static readonly LogLevel Info = new LogLevel("INFO", InfoLevelValue);

		public static readonly LogLevel Debug = new LogLevel("DEBUG", DebugLevelValue);

		///<summary>
		/// A message of some importance
		///</summary>
		public static readonly LogLevel Warning = new LogLevel("WARN", WarningLevelValue);

		///<summary>
		/// Errors, exceptions
		///</summary>
		public static readonly LogLevel Error = new LogLevel("ERROR", ErrorLevelValue);

		///<summary>
		/// Always printed messages (not error's however)
		///</summary>
		public static readonly LogLevel Trace = new LogLevel("TRACE", TraceLevelValue);

		public string Name { get; private set; }

		public int Value { get; private set; }

		public override bool Equals(object obj) {
			LogLevel level = obj as LogLevel;
			return level != null && Value == level.Value;
		}

		public override int GetHashCode() {
			return Value.GetHashCode();
		}

		public override string ToString() {
			return Name;
		}

		public static bool operator >(LogLevel a, LogLevel b) {
			return a.Value > b.Value;
		}

		public static bool operator >(LogLevel a, int value) {
			return a.Value > value;
		}

		public static bool operator <(LogLevel a, LogLevel b) {
			return a.Value < b.Value;
		}

		public static bool operator <(LogLevel a, int value) {
			return a.Value < value;
		}

		public static bool operator >=(LogLevel a, LogLevel b) {
			return a.Value >= b.Value;
		}

		public static bool operator >=(LogLevel a, int value) {
			return a.Value >= value;
		}

		public static bool operator <=(LogLevel a, LogLevel b) {
			return a.Value <= b.Value;
		}

		public static bool operator <=(LogLevel a, int value) {
			return a.Value <= value;
		}

		public static bool operator ==(LogLevel a, LogLevel b) {
			if ((object)a == null && (object)b == null)
				return true;
			if ((object)a == null)
				return false;

			return a.Equals(b);
		}

		public static bool operator ==(LogLevel a, int value) {
			if ((object)a == null)
				return false;

			return a.Value == value;
		}

		public static bool operator !=(LogLevel a, LogLevel b) {
			return !(a == b);
		}

		public static bool operator !=(LogLevel a, int value) {
			return !(a == value);
		}
	}
}