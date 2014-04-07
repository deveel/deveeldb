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
using System.Security.Cryptography;

namespace Deveel.Data.Security {
	public static class HashFunctions {
		public static IHashFunction Sha1 {
			get { return new HashFunction("SHA1"); }
		}

		public static IHashFunction Sha256 {
			get { return new HashFunction("SHA256"); }
		}

		public static IHashFunction Sha384 {
			get { return new HashFunction("SHA384"); }
		}

		public static IHashFunction Sha512 {
			get { return new HashFunction("SHA512"); }
		}

		public static IHashFunction Md5 {
			get { return new HashFunction("MD5"); }
		}

		public static IKeyedHashFunction HmacSha256 {
			get { return new KeyedHashFunction("HMACSHA256"); }
		}

		public static IKeyedHashFunction HmacSha384 {
			get { return new KeyedHashFunction("HMACSHA384"); }
		}

		public static IKeyedHashFunction HmacSha512 {
			get { return new KeyedHashFunction("HMACSHA512"); }
		}

		public static IKeyedHashFunction HmacSha1 {
			get { return new KeyedHashFunction("HMACSHA1");}
		}

		public static IKeyedHashFunction HmacMd5 {
			get { return new KeyedHashFunction("HMACMD5"); }
		}

		public static IHashFunction GetFunction(string functionName) {
			if (String.Equals(functionName, "SHA256", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(functionName, "SHA-256", StringComparison.OrdinalIgnoreCase))
				return Sha256;

			if (String.Equals(functionName, "SHA384", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "SHA-384", StringComparison.OrdinalIgnoreCase))
				return Sha384;

			if (String.Equals(functionName, "SHA512", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "SHA-512", StringComparison.OrdinalIgnoreCase))
				return Sha512;

			if (String.Equals(functionName, "MD5", StringComparison.OrdinalIgnoreCase))
				return Md5;

			if (String.Equals(functionName, "SHA1", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "SHA-1", StringComparison.OrdinalIgnoreCase))
				return Sha1;

			// HMAC
			if (String.Equals(functionName, "HMAC-SHA256", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA256", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(functionName, "HMAC-SHA-256", StringComparison.OrdinalIgnoreCase))
				return HmacSha256;

			if (String.Equals(functionName, "HMAC-SHA384", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA384", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMAC-SHA-384", StringComparison.OrdinalIgnoreCase))
				return HmacSha384;

			if (String.Equals(functionName, "HMAC-SHA512", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA512", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMAC-SHA-512", StringComparison.OrdinalIgnoreCase))
				return HmacSha512;

			if (String.Equals(functionName, "HMAC-SHA1", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA1", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMAC-SHA-1", StringComparison.OrdinalIgnoreCase))
				return HmacSha1;

			if (String.Equals(functionName, "HMAC-MD5", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACMD5", StringComparison.OrdinalIgnoreCase))
				return HmacMd5;

			return null;
		}

		#region HashFunction

		class HashFunction : IHashFunction {
			public HashFunction(string hashName) {
				Hash = HashAlgorithm.Create(hashName);

				if (Hash == null)
					throw new ArgumentException(String.Format("Hash function {0} is not supported", hashName));
			}

			private HashAlgorithm Hash { get; set; }

			public void Dispose() {
				Hash.Clear();
			}

			public int HashSize {
				get { return Hash.HashSize; }
			}

			public byte[] Compute(byte[] data) {
				return Hash.ComputeHash(data);
			}
		}

		#endregion

		#region KeyedHashFunction

		class KeyedHashFunction : IKeyedHashFunction {
			public KeyedHashFunction(string hashName) {
				Hash = KeyedHashAlgorithm.Create(hashName);

				if (Hash == null)
					throw new ArgumentException(String.Format("The hash function {0} is not supported", hashName));
			}

			private KeyedHashAlgorithm Hash { get; set; }

			public void Dispose() {
				Hash.Clear();
			}

			public int HashSize {
				get { return Hash.HashSize; }
			}

			public byte[] Compute(byte[] data) {
				return Hash.ComputeHash(data);
			}

			public byte[] Key {
				get { return Hash.Key; }
				set { Hash.Key = value; }
			}
		}

		#endregion
	}
}