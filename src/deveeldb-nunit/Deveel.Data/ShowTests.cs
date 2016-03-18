using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ShowTests : ContextBasedTest {
		[Test]
		public void ShowSchema() {
			var result = Query.ShowSchema();

			Assert.IsNotNull(result);

			// TODO: Verify the list of schema is coherent
		}
	}
}
