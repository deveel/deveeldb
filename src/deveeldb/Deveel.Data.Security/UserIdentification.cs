// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public sealed class UserIdentification {
		public UserIdentification(string method) {
			if (String.IsNullOrEmpty(method))
				throw new ArgumentNullException("method");

			Method = method;
			Arguments = new Dictionary<string, object>();
		}

		static UserIdentification() {
			PlainText = new UserIdentification("plain");
		}

		public string Method { get; private set; }

		public IDictionary<string, object> Arguments { get; private set; }

		public static UserIdentification PlainText { get; private set; }

		public static UserIdentification Pkcs12(string salt) {
			var id = new UserIdentification("hash");
			id.Arguments["salt"] = salt;
			id.Arguments["mechanism"] = "pkcs12";
			return id;
		}
	}
}
