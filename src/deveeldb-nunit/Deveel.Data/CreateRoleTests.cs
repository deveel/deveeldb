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
	public sealed class CreateRoleTests : ContextBasedTest {
		protected override void OnBeforeTearDown(string testName) {
			if (testName == "NewRole")
				base.OnBeforeTearDown(testName);
		}

		[Test]
		public void NewRole() {
			AdminQuery.CreateRole("db_admin");

			var exists = AdminQuery.Access().RoleExists("db_admin");
			Assert.IsTrue(exists);
		}

		[Test]
		public void ExistingRole() {
			Assert.Throws<StatementException>(() => AdminQuery.CreateRole("secure_access"));
		}

		[Test]
		public void SystemRole() {
			Assert.Throws<StatementException>(() => AdminQuery.CreateRole(SystemRoles.SecureAccessRole));
		}
	}
}
