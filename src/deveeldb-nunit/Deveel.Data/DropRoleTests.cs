using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropRoleTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateRole("test_role");
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropRole("test_role");
			return true;
		}

		[Test]
		public void ExistingRole() {
			Query.DropRole("test_role");

			var exists = Query.Access().RoleExists("test_role");

			Assert.IsFalse(exists);
		}

		[Test]
		public void NotExistingRole() {
			Assert.Throws<StatementException>(() => Query.DropRole("another_role"));
		}

		[Test]
		public void SystemRole() {
			Assert.Throws<SecurityException>(() => Query.DropRole(SystemRoles.SecureAccessRole));

			var exists = Query.Access().RoleExists(SystemRoles.SecureAccessRole);
			Assert.IsTrue(exists);
		}
	}
}
