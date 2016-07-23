using System;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class GrantStringFormatTests {
		[Test]
		public static void SinglePrivilege_WithNoGrantOption() {
			var statement = new GrantPrivilegesStatement("user1", Privileges.Select, ObjectName.Parse("t1"));

			var sql = statement.ToString();
			var expected = "GRANT SELECT TO user1 ON t1";
			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void MultiplePrivileges_WithGrantOption() {
			var privs = Privileges.Update | Privileges.Insert;
			var statement = new GrantPrivilegesStatement("admin", privs, true, ObjectName.Parse("APP.test1"));

			var sql = statement.ToString();
			var expected = "GRANT UPDATE, INSERT TO admin ON APP.test1 WITH GRANT OPTION";

			Assert.AreEqual(expected, sql);
		}
	}
}
