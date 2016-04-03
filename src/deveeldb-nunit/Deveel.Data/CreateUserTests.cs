using System;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateUserTests : ContextBasedTest {
		[Test]
		public void WithSimplePassword() {
			const string userName = "tester";

			Query.CreateUser(userName, "12345");

			var exists = Query.Session.Access().UserExists(userName);
			Assert.IsTrue(exists);
		}

		[Test]
		public void ExistingUser() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(AdminUserName, "0123456789"));
		}

		[Test]
		public void PublicUser() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(User.PublicName, "12345"));
		}

		[Test]
		public void WithRoleName() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(SystemRoles.LockedRole, "0123456789"));
		}
	}
}
