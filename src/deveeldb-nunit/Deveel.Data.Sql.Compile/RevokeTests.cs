using System;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class RevokeTests : SqlCompileTestBase {
		[Test]
		public void RevokeFromOneUserOnAnObject() {
			const string sql = "REVOKE UPDATE, INSERT ON test_table FROM test_user";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<RevokePrivilegesStatement>(statement);

			var revoke = (RevokePrivilegesStatement) statement;

			Assert.AreEqual("test_user", revoke.Grantee);
			Assert.IsNotNull(revoke.ObjectName);
			Assert.AreEqual("test_table", revoke.ObjectName.FullName);
			Assert.AreEqual(Privileges.Insert | Privileges.Update, revoke.Privileges);
		}
	}
}
