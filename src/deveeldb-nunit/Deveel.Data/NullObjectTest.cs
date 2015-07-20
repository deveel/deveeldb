using System;

using Deveel.Data.Types;

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

			DataObject result = null;
			Assert.DoesNotThrow(() => result = obj == null);
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<BooleanType>(result.Type);
			Assert.IsTrue(result.IsNull);
		}
	}
}