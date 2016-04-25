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
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class GrantTests : SqlCompileTestBase {
		[Test]
		public void ParseGrantToOneUserToOneTable() {
			const string sql = "GRANT SELECT, DELETE, UPDATE ON test_table TO test_user";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			Assert.IsInstanceOf<GrantPrivilegesStatement>(result.Statements.ElementAt(0));

			var first = (GrantPrivilegesStatement) result.Statements.ElementAt(0);
			Assert.AreEqual(Privileges.Select | Privileges.Delete | Privileges.Update, first.Privilege);
			Assert.AreEqual("test_user", first.Grantee);
			Assert.AreEqual("test_table", first.ObjectName.ToString());
		}

		[Test]
		public void ParseGrantRolesToOneUser() {
			const string sql = "GRANT admin, data_reader TO test_user";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			Assert.IsInstanceOf<GrantRoleStatement>(result.Statements.ElementAt(0));
			Assert.IsInstanceOf<GrantRoleStatement>(result.Statements.ElementAt(1));

			var first = (GrantRoleStatement) result.Statements.ElementAt(0);
			Assert.AreEqual("admin", first.Role);
			Assert.AreEqual("test_user", first.Grantee);

			var second = (GrantRoleStatement)result.Statements.ElementAt(1);
			Assert.AreEqual("data_reader", second.Role);
			Assert.AreEqual("test_user", second.Grantee);
		}
	}
}