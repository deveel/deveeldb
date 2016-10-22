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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class GrantTests : ContextBasedTest {
		private static void CreateTestUser(IQuery query) {
			query.Session.Access().CreateUser("test_user", "12345");
		}

		private static void CreateTestTable(IQuery query) {
			var tableName1 = ObjectName.Parse("APP.test_table");

			query.Access().CreateTable(table => table
				.Named(tableName1)
				.WithColumn(column => column
					.Named("id")
					.HavingType(PrimitiveTypes.Integer())
					.WithDefault(SqlExpression.FunctionCall("UNIQUEKEY",
						new SqlExpression[] {SqlExpression.Constant(tableName1.FullName)})))
				.WithColumn("first_name", PrimitiveTypes.String())
				.WithColumn("last_name", PrimitiveTypes.String())
				.WithColumn("birth_date", PrimitiveTypes.DateTime())
				.WithColumn("active", PrimitiveTypes.Boolean()));

			query.Access().AddPrimaryKey(tableName1, "id", "PK_TEST_TABLE");

			query.Access().CreateTable(table => table
				.Named("APP.test_table2")
				.WithColumn("person_id", PrimitiveTypes.Integer())
				.WithColumn("value", PrimitiveTypes.Boolean()));
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestUser(query);
			CreateTestRole(query);
			CreateTestTable(query);
			return true;
		}

		private void CreateTestRole(IQuery query) {
			query.Session.Access().CreateRole("test_role");
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DeleteUser("test_user");
			query.Access().DropRole("test_role");

			var tableName1 = ObjectName.Parse("APP.test_table");
			var tableName2 = ObjectName.Parse("APP.test_table2");

			query.Access().DropAllTableConstraints(tableName1);
			query.Access().DropObject(DbObjectType.Table, tableName1);
			query.Access().DropObject(DbObjectType.Table, tableName2);
			return true;
		}

		[Test]
		public void GrantCreateToUserOnSchema() {
			AdminQuery.Grant("test_user", Privileges.Create, new ObjectName("APP"));

			// TODO: Query the user privileges and assert he was granted
		}

		[Test]
		public void GrantSelectToUserOnTable() {
			AdminQuery.Grant("test_user", Privileges.Create, new ObjectName("APP.test_table"));

			// TODO: Query the user privileges and assert he was granted
		}

		[Test]
		public void GrantCreateToRoleOnSchema() {
			AdminQuery.Grant("test_role", Privileges.Create, new ObjectName("APP"));
		}

		[Test]
		public void GrantRoleToUser() {
			AdminQuery.GrantRole("test_user", "test_role");

			var inRole = AdminQuery.Access().UserIsInRole("test_user", "test_role");
			Assert.IsTrue(inRole);
		}
	}
}
