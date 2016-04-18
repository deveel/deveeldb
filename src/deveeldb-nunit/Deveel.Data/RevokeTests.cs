using System;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class RevokeTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());

			query.Access().CreateTable(tableInfo);

			query.Access().CreateUser("test_user", "0123456789");
			query.Access().GrantOnTable(tableName, "test_user", Privileges.Alter);

			query.Access().CreateRole("test_role");
			query.Access().AddUserToRole("test_user", "test_role");
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void PrivilegeFromUser() {
			var tableName = ObjectName.Parse("APP.test_table");
			Query.Revoke("test_user", Privileges.Alter, ObjectName.Parse("APP.test_table"));

			var hasPrivs = Query.Access().UserHasPrivilege("test_user", DbObjectType.Table, tableName, Privileges.Alter);
			Assert.IsFalse(hasPrivs);
		}

		[Test]
		public void RoleFromUser() {
			Query.RevokeRole("test_user", "test_role");

			var inRole = Query.Access().UserIsInRole("test_user", "test_role");
			Assert.IsFalse(inRole);
		}
	}
}
