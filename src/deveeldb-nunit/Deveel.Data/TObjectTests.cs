using System;

using Deveel.Data.Sql;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class TObjectTests {
		[Test]
		public void IntegerEqual() {
			TObject obj1 = (TObject) 32;
			TObject obj2 = (TObject) 21;
			Assert.IsTrue(obj1 != obj2);

			obj1 = (TObject) 21;
			obj2 = (TObject) 21;
			Assert.IsTrue(obj1 == obj2);
		}

		[Test]
		public void StringEqual() {
			TObject obj1 = (TObject) "test1";
			TObject obj2 = (TObject) "test2";
			Assert.IsTrue(obj1 != obj2);

			obj1 = (TObject) "test_a";
			obj2 = (TObject) "test_a";
			Assert.IsTrue(obj1 != null);
			Assert.IsTrue(obj1 == obj2);

			obj2 = new TObject(new TStringType(SqlType.Char, 20, "enUS"), "test_b");
			Assert.IsTrue(obj1 != obj2);
		}

		[Test]
		public void BigNumberEqual() {
			
		}

		[TestCase("1980-10-06")]
		public void DateCast(string date) {
			TObject obj = TObject.CreateAndCastFromObject(TType.GetDateType(SqlType.Date), date);
			Assert.IsNotNull(obj);
			Assert.AreEqual(obj.TType.SqlType, SqlType.Date);
		}
	}
}