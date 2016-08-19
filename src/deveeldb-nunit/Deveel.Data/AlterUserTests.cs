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

namespace Deveel.Data {
	[TestFixture]
	public sealed class AlterUserTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateUser("test_user", "0123456789");
			query.Access().CreateRole("test_role1");
			query.Access().CreateRole("role2");

			if (testName == "Unlock")
				query.Access().SetUserStatus("test_user", UserStatus.Locked);
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropRole("role2");
			query.Access().DropRole("test_role1");
			query.Access().DeleteUser("test_user");
			return true;
		}

		[Test]
		public void SetPassword() {
			AdminQuery.SetPassword("test_user", "1234");

			var authenticated = AdminQuery.Access().Authenticate("test_user", "1234");
			Assert.IsTrue(authenticated);
		}

		[Test]
		public void SetRoles() {
			AdminQuery.SetRoles("test_user", "test_role1", "role2");

			var userRoles = AdminQuery.Access().GetUserRoles("test_user");

			Assert.IsNotNull(userRoles);
			Assert.IsNotEmpty(userRoles);

			var roleNames = userRoles.Select(x => x.Name).ToArray();
			Assert.Contains("test_role1", roleNames);
			Assert.Contains("role2", roleNames);
		}

		[Test]
		public void Unlock() {
			AdminQuery.SetAccountUnlocked("test_user");

			var newStatus = AdminQuery.Access().GetUserStatus("test_user");

			Assert.AreEqual(UserStatus.Unlocked, newStatus);
		}

		[Test]
		public void Lock() {
			AdminQuery.SetAccountLocked("test_user");

			var newStatus = AdminQuery.Access().GetUserStatus("test_user");

			Assert.AreEqual(UserStatus.Locked, newStatus);
		}
	}
}
