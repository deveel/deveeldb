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

namespace Deveel.Diagnostics {
	///<summary>
	/// Debug level static values.
	///</summary>
	public sealed class DebugLevel {
		private DebugLevel(string name, int value) {
			this.name = name;
			this.value = value;
		}

		private readonly int value;
		private readonly string name;

		///<summary>
		/// General processing 'noise'
		///</summary>
		public static readonly DebugLevel Information = new DebugLevel("INFO", 100);

		///<summary>
		/// Query information 'noise'
		///</summary>
		public static readonly DebugLevel QueryInformation = new DebugLevel("QUERY_INFO", 101);

		///<summary>
		/// A message of some importance
		///</summary>
		public static readonly DebugLevel Warning = new DebugLevel("WARN", 20);

		///<summary>
		/// Crackers, etc
		///</summary>
		public static readonly DebugLevel Alert = new DebugLevel("ALERT", 30);

		///<summary>
		/// Errors, exceptions
		///</summary>
		public static readonly DebugLevel Error = new DebugLevel("ERROR", 100);

		///<summary>
		/// Always printed messages (not error's however)
		///</summary>
		public static readonly DebugLevel Message = new DebugLevel("MESSAGE", 10000);

		public string Name {
			get { return name; }
		}

		public int Value {
			get { return value; }
		}

		public override bool Equals(object obj) {
			DebugLevel level = obj as DebugLevel;
			return level != null && value == level.value;
		}

		public override int GetHashCode() {
			return value.GetHashCode();
		}

		public override string ToString() {
			return name;
		}

		internal static DebugLevel Create(string name, int value) {
			return new DebugLevel(name, value);
		}

		public static bool operator >(DebugLevel a, DebugLevel b) {
			return a.value > b.value;
		}

		public static bool operator >(DebugLevel a, int value) {
			return a.value > value;
		}

		public static bool operator <(DebugLevel a, DebugLevel b) {
			return a.value < b.value;
		}

		public static bool operator <(DebugLevel a, int value) {
			return a.value < value;
		}

		public static bool operator >=(DebugLevel a, DebugLevel b) {
			return a.value >= b.value;
		}

		public static bool operator >=(DebugLevel a, int value) {
			return a.value >= value;
		}

		public static bool operator <=(DebugLevel a, DebugLevel b) {
			return a.value <= b.value;
		}

		public static bool operator <=(DebugLevel a, int value) {
			return a.value <= value;
		}

		public static bool operator ==(DebugLevel a, DebugLevel b) {
			return a.Equals(b);
		}

		public static bool operator ==(DebugLevel a, int value) {
			return a.value == value;
		}

		public static bool operator !=(DebugLevel a, DebugLevel b) {
			return !(a == b);
		}

		public static bool operator !=(DebugLevel a, int value) {
			return !(a == value);
		}
	}
}