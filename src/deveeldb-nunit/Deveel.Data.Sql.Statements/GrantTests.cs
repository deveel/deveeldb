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
using System.Linq;

using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class GrantTests : ContextBasedTest {
		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);
			query.CreateUser("test_user", "12345");
			return query;
		}

		[Test]
		public void ParseGrantToOneUserToOneTable() {
			const string sql = "GRANT SELECT, DELETE, UPDATE PRIVILEGE ON test_table TO test_user";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(3, statements.Count());
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(1));
			Assert.IsInstanceOf<GrantPrivilegesStatement>(statements.ElementAt(2));

			var first = (GrantPrivilegesStatement) statements.ElementAt(0);
			Assert.AreEqual(Privileges.Select, first.Privilege);
			Assert.AreEqual("test_user", first.Grantee);
			Assert.AreEqual("test_table", first.ObjectName.ToString());
		}

		[Test]
		public void ParseGrantRolesToOneUser() {
			const string sql = "GRANT admin, data_reader TO test_user";

			var statements = SqlStatement.Parse(sql);
			Assert.IsNotNull(statements);
			Assert.IsNotEmpty(statements);
			Assert.AreEqual(2, statements.Count());
			Assert.IsInstanceOf<GrantRoleStatement>(statements.ElementAt(0));
			Assert.IsInstanceOf<GrantRoleStatement>(statements.ElementAt(1));

			var first = (GrantRoleStatement) statements.ElementAt(0);
			Assert.AreEqual("admin", first.Role);
			Assert.AreEqual("test_user", first.UserName);
		}
	}
}
