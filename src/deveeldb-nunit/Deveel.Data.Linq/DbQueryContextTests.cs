using System;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public sealed class DbQueryContextTests : ContextBasedTest {
		[Test]
		public void CreateNew() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new DbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);
		}
	}
}
