using System;
using System.Linq;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class GrantTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			query.CreateUser("test_user", "12345");
			return query;
		}

		[Test]
		public void ParseGrantToOneUserToOneTable() {
			const string sql = "GRANT SELECT, DELETE, UPDATE PRIVILEGE ON test_table TO test_user";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(3, statements.Count());
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(1));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(2));

			var first = (GrantPrivilegesStatement) statements.ElementAt(0);
			Assert.AreEqual(Privileges.Select, first.Privilege);
			Assert.AreEqual("test_user", first.Grantee);
			Assert.AreEqual("test_table", first.ObjectName.ToString());
		}

		[Test]
		public void ParseGrantRolesToOneUser() {
			const string sql = "GRANT admin, data_reader TO test_user";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(2, statements.Count());
			Assert.IsInstanceOf<GrantRoleStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<GrantRoleStatement>(statements.ElementAt(1));

			var first = (GrantRoleStatement) statements.ElementAt(0);
			Assert.AreEqual("admin", first.Role);
			Assert.AreEqual("test_user", first.UserName);
		}
	}
}
