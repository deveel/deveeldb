using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class ObjectNameTests : ContextBasedTest {
		[Test]
		public void ResolveSchemaName() {
			var resolved = Query.IsolatedAccess.ResolveObjectName("APP");

			Assert.IsNotNull(resolved);
			Assert.IsNull(resolved.Parent);
			Assert.IsNotNull(resolved.Name);
			Assert.AreEqual("APP", resolved.Name);
		}
	}
}
