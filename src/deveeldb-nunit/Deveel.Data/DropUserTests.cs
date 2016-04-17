using System;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropUserTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateUser("tester", "12345");
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DeleteUser("tester");
			return true;
		}

		[Test]
		public void Existing() {
			Query.DropUser("tester");

			var exists = Query.Session.Access().UserExists("tester");
			Assert.IsFalse(exists);
		}

		[Test]
		public void PublicUser() {
			Assert.Throws<SecurityException>(() => Query.DropUser(User.PublicName));
		}

		[Test]
		public void SystemUser() {
			Assert.Throws<SecurityException>(() => Query.DropUser(User.SystemName));
		}
	}
}
