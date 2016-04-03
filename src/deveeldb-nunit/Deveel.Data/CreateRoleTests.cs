using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateRoleTests : ContextBasedTest {
		[Test]
		public void NewRole() {
			Query.CreateRole("db_admin");

			var exists = Query.Access().RoleExists("db_admin");
			Assert.IsTrue(exists);
		}

		[Test]
		public void ExistingRole() {
			Assert.Throws<StatementException>(() => Query.CreateRole("secure_access"));
		}

		[Test]
		public void SystemRole() {
			Assert.Throws<StatementException>(() => Query.CreateRole(SystemRoles.SecureAccessRole));
		}
	}
}
