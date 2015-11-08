using System;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class DataTypeParseTests : ContextBasedTest {
		[Test]
		public void SimpleNumeric() {
			const string typeString = "NUMERIC";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<NumericType>(type);
			Assert.AreEqual(-1, ((NumericType)type).Size);
		}
	}
}
