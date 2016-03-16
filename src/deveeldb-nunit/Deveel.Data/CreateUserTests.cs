using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateUserTests : ContextBasedTest {
		[Test]
		public void WithSimplePassword() {
			const string userName = "tester";

			Query.CreateUser(userName, "12345");

			var exists = Query.Session.Access.UserExists(userName);
			Assert.IsTrue(exists);
		}
	}
}
