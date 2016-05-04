// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Security {
	public sealed class Pkcs12UserIdentifier : IUserIdentifier {
		public string Name {
			get { return KnownUserIdentifications.Pkcs12; }
		}

		public bool VerifyIdentification(string input, UserIdentification identification) {
			string salt;
			object arg;
			if (!identification.Arguments.TryGetValue("salt", out arg)) {
				salt = HashFunctions.HmacSha512.GenerateSaltString();
			} else {
				salt = arg.ToString();
			}

			return HashFunctions.HmacSha512.VerifyPbkdf2String(identification.Token, input, salt);
		}

		public UserIdentification CreateIdentification(string input) {
			var salt = HashFunctions.HmacSha512.GenerateSaltString();
			var token = HashFunctions.HmacSha512.MakePbkdf2String(input, salt, 512);
			var id = new UserIdentification(KnownUserIdentifications.Pkcs12, token);
			id.Arguments.Add("salt", salt);
			return id;
		}
	}
}
