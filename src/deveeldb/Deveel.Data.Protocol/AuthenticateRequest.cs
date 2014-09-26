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

namespace Deveel.Data.Protocol {
	[Serializable]
	public sealed class AuthenticateRequest : IMessage {
		public AuthenticateRequest(string userName, string password) 
			: this(userName, password, false) {
		}

		public AuthenticateRequest(string userName, string password, bool ecrypt) 
			: this(null, userName, password, ecrypt) {
		}

		public AuthenticateRequest(string defaultSchema, string userName, string password) 
			: this(defaultSchema, userName, password, false) {
		}

		public AuthenticateRequest(string defaultSchema, string userName, string password, bool ecrypt) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			DefaultSchema = defaultSchema;
			UserName = userName;
			Password = password;
			Ecrypt = ecrypt;
		}

		public string DefaultSchema { get; private set; }

		public string UserName { get; private set; }

		public string Password { get; private set; }

		public bool Ecrypt { get; private set; }
	}
}