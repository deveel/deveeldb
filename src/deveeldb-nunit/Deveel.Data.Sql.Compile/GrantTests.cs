using System;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class GrantTests : SqlCompileTestBase {
		[Test]
		public void ParseGrantToOneUserToOneTable() {
			const string sql = "GRANT SELECT, DELETE, UPDATE ON test_table TO test_user";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			Assert.IsInstanceOf<GrantPrivilegesStatement>(result.Statements.ElementAt(0));

			var first = (GrantPrivilegesStatement) result.Statements.ElementAt(0);
			Assert.AreEqual(Privileges.Select | Privileges.Delete | Privileges.Update, first.Privilege);
			Assert.AreEqual("test_user", first.Grantee);
			Assert.AreEqual("test_table", first.ObjectName.ToString());
		}

		[Test]
		public void ParseGrantRolesToOneUser() {
			const string sql = "GRANT admin, data_reader TO test_user";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			Assert.IsInstanceOf<GrantRoleStatement>(result.Statements.ElementAt(0));
			Assert.IsInstanceOf<GrantRoleStatement>(result.Statements.ElementAt(1));

			var first = (GrantRoleStatement) result.Statements.ElementAt(0);
			Assert.AreEqual("admin", first.Role);
			Assert.AreEqual("test_user", first.UserName);

			var second = (GrantRoleStatement)result.Statements.ElementAt(1);
			Assert.AreEqual("data_reader", second.Role);
			Assert.AreEqual("test_user", second.UserName);
		}
	}
}