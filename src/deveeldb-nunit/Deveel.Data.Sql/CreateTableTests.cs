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
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CreateTableTests : ContextBasedTest {
		private const string TestUserName = "test";
		private const string TestPassword = "abc1234";

		protected override ISession CreateAdminSession(IDatabase database) {
			using (var session = database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var query = session.CreateQuery()) {
					if (TestContext.CurrentContext.Test.Name.Equals("CreateSimple_RegularUser")) {
						var user = query.CreateUser(TestUserName, TestPassword);
						//queryContext.GrantHostAccessToUser(TestUserName, KnownConnectionProtocols.Local, "%");
						query.GrantToUserOnSchema("APP", user.Name, Privileges.Create);
						query.Commit();
					}
				}
			}

			return base.CreateAdminSession(database);
		}

		[Test]
		public void CreateSimpleTableAsAdmin_NewSession() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			using (var session = CreateUserSession(AdminUserName, AdminPassword)) {
				using (var query = session.CreateQuery()) {
					Assert.DoesNotThrow(() => query.CreateTable(tableInfo));
					Assert.DoesNotThrow(() => query.Commit());
				}
			}

			using (var session = CreateUserSession(AdminUserName, AdminPassword)) {
				using (var query = session.CreateQuery()) {
					bool exists = false;
					Assert.DoesNotThrow(() => exists = query.TableExists(tableName));
					Assert.IsTrue(exists);
				}
			}
		}

		[Test]
		public void CreateSimple_RegularUser() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			using (var session = CreateUserSession(TestUserName, TestPassword)) {
				using (var query = session.CreateQuery()) {
					Assert.DoesNotThrow(() => query.CreateTable(tableInfo));
				}
			}
		}
	}
}
