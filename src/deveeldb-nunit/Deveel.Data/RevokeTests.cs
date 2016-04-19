// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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
			query.Access().RevokeAllGrantsOn(DbObjectType.Table, tableName);
			query.Access().DropRole("test_role");
			query.Access().DeleteUser("test_user");
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
