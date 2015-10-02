using System;

using Deveel.Data;
using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CreateTableTests : ContextBasedTest {
		private const string TestUserName = "test";
		private const string TestPassword = "abc1234";

		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var queryContext = base.CreateQueryContext(database);

			if (TestContext.CurrentContext.Test.Name.Equals("CreateSimple_RegularUser")) {
				var user = queryContext.CreateUser(TestUserName, TestPassword);
				queryContext.GrantHostAccessToUser(TestUserName, KnownConnectionProtocols.Local, "%");
				queryContext.GrantToUserOnSchemaTables("APP", user, User.System, Privileges.Create);
				queryContext.Session.Commit();
			}

			return queryContext;
		}

		[Test]
		public void CreateSimpleTableAsAdmin_NewSession() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					Assert.DoesNotThrow(() => context.CreateTable(tableInfo));
				}

				Assert.DoesNotThrow(() => session.Commit());
			}

			using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					bool exists = false;
					Assert.DoesNotThrow(() => exists = context.TableExists(tableName));
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

			using (var session = Database.CreateUserSession(TestUserName, TestPassword)) {
				using (var context = new SessionQueryContext(session)) {
					Assert.DoesNotThrow(() => context.CreateTable(tableInfo));
				}

				Assert.DoesNotThrow(() => session.Commit());
			}
		}
	}
}
