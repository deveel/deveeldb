using System;

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;
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
				//queryContext.GrantHostAccessToUser(TestUserName, KnownConnectionProtocols.Local, "%");
				queryContext.GrantToUserOnSchema("APP", user.Name, Privileges.Create);
				queryContext.Commit();
			}

			return queryContext;
		}

		[Test]
		public void CreateSimpleTableAsAdmin_NewSession() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			using (var context = Database.CreateQueryContext(AdminUserName, AdminPassword)) {
				Assert.DoesNotThrow(() => context.CreateTable(tableInfo));
				Assert.DoesNotThrow(() => context.Commit());
			}

			using (var context = Database.CreateQueryContext(AdminUserName, AdminPassword)) {
				bool exists = false;
				Assert.DoesNotThrow(() => exists = context.TableExists(tableName));
				Assert.IsTrue(exists);
			}
		}

		[Test]
		public void CreateSimple_RegularUser() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			var context = CreateUserQueryContext(TestUserName, TestPassword);

			Assert.DoesNotThrow(() => context.CreateTable(tableInfo));

			context.Dispose();
		}
	}
}
