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

namespace Deveel.Data.Control {
	/// <summary>
	/// An object the describes a single configuration property and the 
	/// default value for it.
	/// </summary>
	public sealed class ConfigProperty : ICloneable {

		private readonly string key;
		private readonly string value;
		private readonly string comment;

		internal ConfigProperty(string key, string default_value, string comment) {
			this.key = key;
			this.value = default_value;
			this.comment = comment;
		}

		public string Key {
			get { return key; }
		}

		public string Value {
			get { return value; }
		}

		public string Comment {
			get { return comment; }
		}

		public object Clone() {
			return MemberwiseClone();
		}
	}
}