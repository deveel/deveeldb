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
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Deveel.Data.Security {
	public static class CryptoHashExtenions {
		#region Pbkdf2

		private const int DefaultIterationCount = 10000;

		public static string MakePbkdf2String(this IKeyedHashFunction hash, string input, string salt, int length) {
			return MakePbkdf2String(hash, input, salt, length, DefaultIterationCount);
		}

		public static string MakePbkdf2String(this IKeyedHashFunction hash, string input, string salt, int length, int iterationCount) {
			return hash.MakePbkdf2String(input, Convert.FromBase64String(salt), length, iterationCount);
		}

		public static string MakePbkdf2String(this IKeyedHashFunction hash, string input, byte[] salt, int length) {
			return MakePbkdf2String(hash, input, salt, length, DefaultIterationCount);
		}

		public static string MakePbkdf2String(this IKeyedHashFunction hash, string input, byte[] salt, int length, int iterationCount) {
			var data = Encoding.UTF8.GetBytes(input);
			var result = hash.MakePbkdf2(data, salt, length, iterationCount);
			return Convert.ToBase64String(result);
		}

		public static byte[] MakePbkdf2(this IKeyedHashFunction hash, byte[] salt, int length, int iterationCount) {
			var hashKey = hash.Key;
			if (hashKey == null || hashKey.Length == 0)
				throw new InvalidOperationException("Key was not specified for the hash.");

			return MakePbkdf2(hash, hashKey, salt, length, iterationCount);
		}

		public static byte[] MakePbkdf2(this IKeyedHashFunction hash, byte[] data, byte[] salt, int length) {
			return MakePbkdf2(hash, data, salt, length, DefaultIterationCount);
		}

		public static byte[] MakePbkdf2(this IKeyedHashFunction hash, byte[] data, byte[] salt, int length, int iterationCount) {
			hash.Key = data;

			int hashLength = hash.HashSize/8;
			if ((hash.HashSize & 7) != 0)
				hashLength++;

			int keyLength = length/hashLength;
			if (length > (0xFFFFFFFFL*hashLength) || length < 0)
				throw new ArgumentOutOfRangeException("length");

			if (length%hashLength != 0)
				keyLength++;

			byte[] extendedkey = new byte[salt.Length + 4];
			Buffer.BlockCopy(salt, 0, extendedkey, 0, salt.Length);

			using (var ms = new MemoryStream()) {
				for (int i = 0; i < keyLength; i++) {
					extendedkey[salt.Length] = (byte) (((i + 1) >> 24) & 0xFF);
					extendedkey[salt.Length + 1] = (byte) (((i + 1) >> 16) & 0xFF);
					extendedkey[salt.Length + 2] = (byte) (((i + 1) >> 8) & 0xFF);
					extendedkey[salt.Length + 3] = (byte) (((i + 1)) & 0xFF);

					byte[] u = hash.Compute(extendedkey);
					Array.Clear(extendedkey, salt.Length, 4);

					byte[] f = u;
					for (int j = 1; j < iterationCount; j++) {
						u = hash.Compute(u);
						for (int k = 0; k < f.Length; k++) {
							f[k] ^= u[k];
						}
					}

					ms.Write(f, 0, f.Length);
					Array.Clear(u, 0, u.Length);
					Array.Clear(f, 0, f.Length);
				}

				byte[] dk = new byte[length];
				ms.Position = 0;
				ms.Read(dk, 0, length);
				ms.Position = 0;

				for (long i = 0; i < ms.Length; i++) {
					ms.WriteByte(0);
				}

				Array.Clear(extendedkey, 0, extendedkey.Length);
				return dk;
			}
		}

		public static bool VerifyPbkdf2(this IKeyedHashFunction hash, byte[] hashed, byte[] otherData, byte[] salt) {
			return VerifyPbkdf2(hash, hashed, otherData, salt, DefaultIterationCount);
		}

		public static bool VerifyPbkdf2(this IKeyedHashFunction hash, byte[] hashed, byte[] otherData, byte[] salt, int iterationCount) {
			int length = hashed.Length;

			var otherHashed = hash.MakePbkdf2(otherData, salt, length, iterationCount);

			if (otherHashed.Length != hashed.Length)
				return false;

			return ByteArraysEqual(hashed, otherHashed);
		}

		public static bool VerifyPbkdf2String(this IKeyedHashFunction hash, string hashedString, string otherString, byte[] salt) {
			return VerifyPbkdf2String(hash, hashedString, otherString, salt, DefaultIterationCount);
		}

		public static bool VerifyPbkdf2String(this IKeyedHashFunction hash, string hashedString, string otherString, byte[] salt, int iterationCount) {
			return hash.VerifyPbkdf2String(hashedString, otherString, Convert.ToBase64String(salt), iterationCount);
		}

		public static bool VerifyPbkdf2String(this IKeyedHashFunction hash, string hashedString, string otherString, string saltString) {
			return VerifyPbkdf2String(hash, hashedString, otherString, saltString, DefaultIterationCount);
		}

		public static bool VerifyPbkdf2String(this IKeyedHashFunction hash, string hashedString, string otherString, string saltString, int iterationCount) {
			var hashed = Convert.FromBase64String(hashedString);
			var otherData = Encoding.UTF8.GetBytes(otherString);
			var salt = Convert.FromBase64String(saltString);

			return hash.VerifyPbkdf2(hashed, otherData, salt, iterationCount);
		}

		private static bool ByteArraysEqual(byte[] a, byte[] b) {
			if (ReferenceEquals(a, b)) {
				return true;
			}

			if (a == null || b == null || a.Length != b.Length) {
				return false;
			}

			bool areSame = true;
			for (int i = 0; i < a.Length; i++) {
				areSame &= (a[i] == b[i]);
			}
			return areSame;
		}

		#endregion

		public static string ComputeString(this IHashFunction hash, string s) {
			var data = Encoding.UTF8.GetBytes(s);
			return BinaryToHex(hash.Compute(data));
		}

		public static byte[] GenerateSalt(this IHashFunction hash) {
			int byteLength = (hash.HashSize / 2) / 8;
			byte[] buf = new byte[byteLength];
			var rng = new RNGCryptoServiceProvider();
			rng.GetBytes(buf);
			return buf;
		}

		public static string GenerateSaltString(this IHashFunction hash) {
			return Convert.ToBase64String(hash.GenerateSalt());
		}

		private static string BinaryToHex(byte[] data) {
			char[] hex = new char[data.Length * 2];

			for (int iter = 0; iter < data.Length; iter++) {
				byte hexChar = ((byte)(data[iter] >> 4));
				hex[iter * 2] = (char)(hexChar > 9 ? hexChar + 0x37 : hexChar + 0x30);
				hexChar = ((byte)(data[iter] & 0xF));
				hex[(iter * 2) + 1] = (char)(hexChar > 9 ? hexChar + 0x37 : hexChar + 0x30);
			}
			return new string(hex);
		}
	}
}