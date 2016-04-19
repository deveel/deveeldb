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
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropRoleTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateRole("test_role");
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropRole("test_role");
			return true;
		}

		[Test]
		public void ExistingRole() {
			Query.DropRole("test_role");

			var exists = Query.Access().RoleExists("test_role");

			Assert.IsFalse(exists);
		}

		[Test]
		public void NotExistingRole() {
			Assert.Throws<StatementException>(() => Query.DropRole("another_role"));
		}

		[Test]
		public void SystemRole() {
			Assert.Throws<SecurityException>(() => Query.DropRole(SystemRoles.SecureAccessRole));

			var exists = Query.Access().RoleExists(SystemRoles.SecureAccessRole);
			Assert.IsTrue(exists);
		}
	}
}
