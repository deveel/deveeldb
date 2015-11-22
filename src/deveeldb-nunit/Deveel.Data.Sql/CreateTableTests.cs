using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CreateTableTests : ContextBasedTest {
		private const string TestUserName = "test";
		private const string TestPassword = "abc1234";

		protected override IUserSession CreateAdminSession(IDatabase database) {
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
