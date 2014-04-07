using System;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class HashTests {
		[TestCase("Hmac-Sha256", "jTr7362S21!.d")]
		[TestCase("Hmac-Sha384", "t%54lpo21.;deçàw")]
		public void Pbkdf2(string function, string text) {
			var hash = HashFunctions.GetFunction(function);

			Assert.IsInstanceOf<IKeyedHashFunction>(hash);

			var keyedHash = hash as IKeyedHashFunction;

			var salt = keyedHash.GenerateSalt();
			var hashed = keyedHash.MakePbkdf2String(text, salt, 32);
			Assert.IsTrue(keyedHash.VerifyPbkdf2String(hashed, text, salt));
		}

		[TestCase("Hmac-Sha256", "kl4536F6£2.d!!l", "lkop08%3!òke")]
		[TestCase("HMAC-Sha-384", "pòe64(31?jkd_", "lkeps:,321é.w")]
		public void Pbkdf2Reuse(string function, string text1, string text2) {
			var hash = HashFunctions.GetFunction(function);

			Assert.IsInstanceOf<IKeyedHashFunction>(hash);

			var keyedHash = hash as IKeyedHashFunction;

			var salt1 = keyedHash.GenerateSalt();
			var hashed1 = keyedHash.MakePbkdf2String(text1, salt1, 32);

			Assert.IsTrue(keyedHash.VerifyPbkdf2String(hashed1, text1, salt1));

			var salt2 = keyedHash.GenerateSalt();
			var hashed2 = keyedHash.MakePbkdf2String(text2, salt2, 32);

			Assert.IsTrue(keyedHash.VerifyPbkdf2String(hashed2, text2, salt2));

			Assert.AreNotEqual(salt1, salt2);
			Assert.AreNotEqual(hashed1, hashed2);
		}

		[TestCase("HmacSha256(32)", "kl4536F6£2.d!!l")]
		[TestCase("HMACSha384(64)", "pòe64(31?jkd_")]
		public void PasswordCryptoTest(string defString, string text) {
			PasswordCrypto crypto = null;
			Assert.DoesNotThrow(() => crypto = PasswordCrypto.Parse(defString));
			Assert.IsNotNull(crypto);

			string salt = null, hashed = null;
			Assert.DoesNotThrow(() => hashed = crypto.Hash(text, out salt));
			Assert.IsNotNull(hashed);
			Assert.IsNotNull(salt);

			Assert.IsTrue(crypto.Verify(hashed, text, salt));
		}
	}
}
