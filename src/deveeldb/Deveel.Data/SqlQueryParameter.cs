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
using System.Text;

using Deveel.Data.Sql;

namespace Deveel.Data {
	[Serializable]
	public sealed class SqlQueryParameter {
		public SqlQueryParameter() 
			: this(null) {
		}

		public SqlQueryParameter(object value) 
			: this(MarkerName, value) {
		}

		public SqlQueryParameter(string name) 
			: this(name, null) {
		}

		public SqlQueryParameter(string name, object value) {
			ValidateName(name);

			Value = value;
			Name = name;
			Direction = ParameterDirection.Input;
			Size = SizeUndefined;
		}

		public const string MarkerName = "?";

		public const char NamePrefix = ':';

		public const int SizeUndefined = -1;

		public string Name { get; private set; }

		public bool IsMarker {
			get { return String.Equals(MarkerName, Name); }
		}

		public object Value { get; set; }

		public int Size { get; set; }

		public ParameterDirection Direction { get; set; }

		// TODO: Auto-discovery by value?
		public SqlType SqlType { get; set; }

		private static void ValidateName(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException();

			if (!String.Equals(MarkerName, name)) {
				if (name.Length < 1)
					throw new ArgumentException();
				if (name[0] == NamePrefix &&
					name.Length < 2)
					throw new ArgumentException();
			}
		}

		public override string ToString() {
			var sb = new StringBuilder();
			if (Direction == ParameterDirection.Input) {
				sb.Append("IN");
			} else if (Direction == ParameterDirection.Output) {
				sb.Append("OUT");
			} else if (Direction == ParameterDirection.InputOutput) {
				sb.Append("INOUT");
			}

			sb.Append(" ");
			sb.Append(Name);
			sb.Append(" = ");
			if (Value == null ||
			    DBNull.Value == Value) {
				sb.Append("NULL");
			} else {
				sb.Append(Value);
			}

			return sb.ToString();
		}
	}
}