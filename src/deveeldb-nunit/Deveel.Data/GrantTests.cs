using System;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class GrantTests : ContextBasedTest {
		private static void CreateTestUser(IQuery query) {
			query.Session.Access().CreateUser("test_user", "12345");
		}

		private static void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			query.Access().CreateTable(tableInfo);
		}

		protected override void OnSetUp(string testName, IQuery query) {
			CreateTestUser(query);
			CreateTestRole(query);
			CreateTestTable(query);
		}

		private void CreateTestRole(IQuery query) {
			query.Session.Access().CreateRole("test_role");
		}

		protected override void OnTearDown(string testName, IQuery query) {
			query.Access().DeleteUser("test_user");
			query.Access().DropRole("test_role");

			var tableName1 = ObjectName.Parse("APP.test_table");
			var tableName2 = ObjectName.Parse("APP.test_table2");

			query.Access().DropAllTableConstraints(tableName1);
			query.Access().DropObject(DbObjectType.Table, tableName1);
			query.Access().DropObject(DbObjectType.Table, tableName2);
		}

		[Test]
		public void GrantCreateToUserOnSchema() {
			Query.Grant("test_user", Privileges.Create, new ObjectName("APP"));

			// TODO: Query the user privileges and assert he was granted
		}

		[Test]
		public void GrantSelectToUserOnTable() {
			Query.Grant("test_user", Privileges.Create, new ObjectName("APP.test_table"));

			// TODO: Query the user privileges and assert he was granted
		}

		[Test]
		public void GrantCreateToRoleOnSchema() {
			Query.Grant("test_role", Privileges.Create, new ObjectName("APP"));
		}

		[Test]
		public void GrantRoleToUser() {
			Query.GrantRole("test_user", "test_role");

			var userRoles = Query.Access().GetUserRoles("test_user");

			Assert.IsNotNull(userRoles);

			var roleNames = userRoles.Select(x => x.Name).ToArray();

			Assert.AreEqual(1, roleNames.Length);
			Assert.Contains("test_role", roleNames);
		}
	}
}
