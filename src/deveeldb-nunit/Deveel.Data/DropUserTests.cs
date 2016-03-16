using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropUserTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			Query.Session.Access.CreateUser("tester", "12345");
		}

		[Test]
		public void Existing() {
			Query.DropUser("tester");

			var exists = Query.Session.Access.UserExists("tester");
			Assert.IsFalse(exists);
		}
	}
}
