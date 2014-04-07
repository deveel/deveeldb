// 
//  Copyright 2014  Deveel
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

namespace Deveel.Data.Security {
	public sealed class PasswordCrypto {
		public PasswordCrypto(string hashName, int keyLength) {
			KeyLength = keyLength;
			HashName = hashName;

			var hash = HashFunctions.GetFunction(hashName);
			if (hash == null)
				throw new ArgumentException(String.Format("Hash function {0} is not supported.", hashName));
			if (!(hash is IKeyedHashFunction))
				throw new ArgumentException(String.Format("Hash function {0} does not handle keys.", hashName));

			HashFunction = hash as IKeyedHashFunction;
		}

		public string HashName { get; private set; }

		public int KeyLength { get; private set; }

		private IKeyedHashFunction HashFunction { get; set; }

		public string Hash(string password, out string salt) {
			salt = HashFunction.GenerateSaltString();
			return HashFunction.MakePbkdf2String(password, salt, 32);
		}

		public bool Verify(string hashedPassword, string password, string salt) {
			return HashFunction.VerifyPbkdf2String(hashedPassword, password, salt);
		}

		public override string ToString() {
			return String.Format("{0}({1})", HashName, KeyLength);
		}

		public static PasswordCrypto Parse(string defString) {
			if (String.IsNullOrEmpty(defString))
				throw new ArgumentNullException("defString");

			var sIndex = defString.IndexOf('(');
			if (sIndex == -1)
				throw new FormatException();

			var eIndex = defString.IndexOf(')');
			if (eIndex == -1)
				throw new FormatException();

			var hashName = defString.Substring(0, sIndex);
			var sKeyLength = defString.Substring(sIndex + 1, eIndex - (sIndex + 1));

			hashName = hashName.Trim();

			if (String.IsNullOrEmpty(hashName))
				throw new FormatException();

			int keyLength;
			if (!Int32.TryParse(sKeyLength, out keyLength))
				throw new FormatException();

			return new PasswordCrypto(hashName, keyLength);
		}
	}
}