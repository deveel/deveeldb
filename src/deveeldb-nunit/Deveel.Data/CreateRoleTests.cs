using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateRoleTests : ContextBasedTest {
		[Test]
		public void NewRole() {
			Query.CreateRole("db_admin");

			// TODO: Assert the role exists
		}

		[Test]
		public void ExistingRole() {
			Assert.Throws<InvalidOperationException>(() => Query.CreateRole("secure_admin"));
		}
	}
}
