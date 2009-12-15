using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CaseTest : TestBase {
		[Test]
		public void StandaloneTest() {
			TObject result = Expression.Evaluate("CASE " +
			                                     "  WHEN true THEN 1" +
			                                     "  ELSE 0");
			Assert.IsTrue(result == (TObject) 1);
		}
	}
}