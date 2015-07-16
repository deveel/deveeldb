using System;

using NUnit;
using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class NullObjectTest {
		[Test]
		public void NullObjectEqualsDbNull() {
			DataObject obj = null;
			Assert.AreEqual(DBNull.Value, (DBNull)obj);
			Assert.IsTrue(obj == DBNull.Value);
		}

		[Test]
		public void NullObjectEqualsNull() {
			DataObject obj = null;
			Assert.AreEqual(null, obj);
			Assert.IsTrue(obj == null);
		}

		[Test]
		public void NotNullObjectEqualsNull() {
			var obj = DataObject.Null();
			Assert.AreEqual(null, obj);
		}
	}
}