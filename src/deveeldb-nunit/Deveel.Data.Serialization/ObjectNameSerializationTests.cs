using System;

using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	[TestFixture]
	public sealed class ObjectNameSerializationTests : SerializationTestBase {
		[Test]
		public void SimpleObjectName() {
			var name = ObjectName.Parse("test_name");

			SerializeAndAssert(name, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("test_name", deserialized.FullName);
			});
		}

		[Test]
		public void ComplexName() {
			var name = ObjectName.Parse("APP.test_name");

			SerializeAndAssert(name, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.Parent);
				Assert.AreEqual("APP", deserialized.ParentName);
				Assert.AreEqual("APP.test_name", deserialized.FullName);
			});
		}
	}
}
