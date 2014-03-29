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
using System.Collections.Generic;
using System.Reflection;
using System.Resources;
using System.Globalization;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed partial class SqlState {
		private readonly string stateClass;
		private readonly string stateSubclass;
		private readonly int sqlCode;
		private readonly string name;
		private string message;

		private static readonly List<SqlState> States = new List<SqlState>();

		public SqlState(int errorCode, string stateClass, string stateSubclass) 
			: this(null, errorCode, stateClass, stateSubclass) {
		}

		public SqlState(string name, int sqlCode, string stateClass, string stateSubclass) {
			if (stateClass == null)
				throw new ArgumentNullException("stateClass");

			this.name = name;
			this.stateClass = stateClass;
			this.stateSubclass = stateSubclass;
			this.sqlCode = sqlCode;
		}

		public SqlState(string name, int sqlCode, string stateString) {
			this.name = name;
			this.sqlCode = sqlCode;

			if (stateString.Length == 2) {
				stateClass = stateString;
				stateSubclass = "000";
			} else if (stateString.Length == 5) {
				stateClass = stateString.Substring(0, 2);
				stateSubclass = stateString.Substring(2, 5);
			} else {
				throw new ArgumentException("SQLSTATE string '" + stateString + "' is not properly format.");
			}
		}

		public string Name {
			get { return name; }
		}

		public int SqlCode {
			get { return sqlCode; }
		}

		public string Class {
			get { return stateClass; }
		}

		public string SubClass {
			get { return stateSubclass; }
		}

		public string MessageFormat {
			get {
				if (String.IsNullOrEmpty(message)) {
					ResourceManager resmgr = new ResourceManager("deveeldb.sqlstates", Assembly.GetExecutingAssembly());
					message = resmgr.GetString(sqlCode.ToString(), CultureInfo.CurrentCulture);
				}

				return message;
			}
		}

		private static void AddState(string name, int code, string stateClass, string stateSubclass) {
			States.Add(new SqlState(name, code, stateClass, stateSubclass));
		}

		private static void AddState(string name, int errorCode, string stateString) {
			States.Add(new SqlState(name, errorCode, stateString));
		}

		public SqlStateException AsException() {
			return AsException(new object[0]);
		}

		public SqlStateException AsException(params object[] args) {
			return new SqlStateException(this, args);
		}

		public string GetMessage(params object[] args) {
			return String.Format(MessageFormat, args);
		}

		public static SqlState GetState(int errorCode) {
			return States.Find(delegate(SqlState state) { return state.SqlCode == errorCode; });
		}

		public static SqlState GetState(string name) {
			return States.Find(
				delegate(SqlState state) {
					return String.Equals(state.Name, name, StringComparison.InvariantCultureIgnoreCase);
				});
		}
	}
}