using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class TObjectTests {
		[Test]
		public void IntegerEqual() {
			TObject obj1 = 32;
			TObject obj2 = 21;
			Assert.IsTrue(obj1 != obj2);

			obj1 = 21;
			obj2 = 21;
			Assert.IsTrue(obj1 == obj2);
		}

		[Test]
		public void StringEqual() {
			TObject obj1 = "test1";
			TObject obj2 = "test2";
			Assert.IsTrue(obj1 != obj2);

			obj1 = "test_a";
			obj2 = "test_a";
			Assert.IsTrue(obj1 != null);
			Assert.IsTrue(obj1 == obj2);

			obj2 = new TObject(new TStringType(SqlType.Char, 20, "enUS"), "test_b");
			Assert.IsTrue(obj1 != obj2);
		}

		[Test]
		public void BigNumberEqual() {
			
		}
	}
}