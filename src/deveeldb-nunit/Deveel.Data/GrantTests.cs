using System;

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
			query.Session.Access.CreateUser("test_user", "12345");
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

			query.Session.Access.CreateTable(tableInfo);
			query.Session.Access.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");

			tableInfo = new TableInfo(ObjectName.Parse("APP.test_table2"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("value", PrimitiveTypes.Boolean());

			query.Session.Access.CreateTable(tableInfo);
		}

		protected override void OnSetUp(string testName) {
			CreateTestUser(Query);
			CreateTestRole(Query);
			CreateTestTable(Query);
		}

		private void CreateTestRole(IQuery query) {
			query.Session.Access.CreateUserGroup("test_role");
		}

		[Test]
		public void GrantCreateToUserOnSchema() {
			Query.Grant("test_user", Privileges.Create, new ObjectName("APP"));

			// TODO: Query the user privileges
		}

		[Test]
		public void GrantSelectToUserOnTable() {
			Query.Grant("test_user", Privileges.Create, new ObjectName("APP.test_table"));

			// TODO: Query the user privileges
		}

		[Test]
		public void GrantCreateToRoleOnSchema() {
			Query.Grant("test_role", Privileges.Create, new ObjectName("APP"));
		}
	}
}
