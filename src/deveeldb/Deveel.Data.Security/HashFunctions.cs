using System;
using System.Security.Cryptography;

namespace Deveel.Data.Security {
	public static class HashFunctions {
		public static IKeyedHashFunction HmacSha256 {
			get { return new HmacSha256Function(); }
		}

		public static IKeyedHashFunction HmacSha384 {
			get { return new HmacSha384HashFunction(); }
		}

		public static IHashFunction GetFunction(string functionName) {
			if (String.Equals(functionName, "HMAC-SHA256", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA256", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(functionName, "HMAC-SHA-256", StringComparison.OrdinalIgnoreCase))
				return HmacSha256;

			if (String.Equals(functionName, "HMAC-SHA384", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMACSHA384", StringComparison.OrdinalIgnoreCase) ||
			    String.Equals(functionName, "HMAC-SHA-384", StringComparison.OrdinalIgnoreCase))
				return HmacSha384;

			return null;
		}

		#region HmacSha26HashFunction

		class HmacSha256Function : IKeyedHashFunction {
			private readonly HMACSHA256 hash;

			public HmacSha256Function() {
				hash = new HMACSHA256();
			}

			public void Dispose() {
				Clear();
			}

			public int HashSize {
				get { return hash.HashSize; }
			}

			public bool CanCompute {
				get { return hash.CanReuseTransform; }
			}

			public byte[] Compute(byte[] data) {
				return hash.ComputeHash(data);
			}

			public void Clear() {
					hash.Clear();
			}

			public byte[] Key {
				get { return hash.Key; }
				set { hash.Key = value; }
			}
		}

		#endregion

		#region HmacSha384HashFunction

		class HmacSha384HashFunction : IKeyedHashFunction {
			private readonly HMACSHA384 hash;

			public HmacSha384HashFunction() {
				hash = new HMACSHA384();
			}

			public void Dispose() {
				hash.Clear();
			}

			public int HashSize {
				get { return hash.HashSize; }
			}

			public byte[] Compute(byte[] data) {
				return hash.ComputeHash(data);
			}

			public void Clear() {
				throw new NotImplementedException();
			}

			public byte[] Key { get; set; }
		}

		#endregion
	}
}