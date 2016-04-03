using System;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class RevokeTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());

			Query.Access().CreateTable(tableInfo);

			Query.Access().CreateUser("test_user", "0123456789");
			Query.Access().GrantOnTable(tableName, "test_user", Privileges.Alter);

			Query.Access().CreateRole("test_role");
			Query.Access().AddUserToRole("test_user", "test_role");
		}

		[Test]
		public void PrivilegeFromUser() {
			var tableName = ObjectName.Parse("APP.test_table");
			Query.Revoke("test_user", Privileges.Alter, ObjectName.Parse("APP.test_table"));

			var hasPrivs = Query.Access().UserHasPrivilege("test_user", DbObjectType.Table, tableName, Privileges.Alter);
			Assert.IsFalse(hasPrivs);
		}
	}
}
