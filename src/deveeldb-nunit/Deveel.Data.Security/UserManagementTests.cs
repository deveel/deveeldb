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

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UserManagementTests : ContextBasedTest {
		protected override ISession CreateAdminSession(IDatabase database) {
			var testName = TestContext.CurrentContext.Test.Name;
			if (testName != "CreateUser") {
				using (var session = base.CreateAdminSession(database)) {
					using (var query = session.CreateQuery()) {
						query.Session.Access.CreateUser("tester", "123456789");
						query.Session.Commit();
					}
				}
			}
			return base.CreateAdminSession(database);
		}

		[Test]
		public void Authenticate_Success() {
			User user = null;

			Assert.DoesNotThrow(() => user = Database.Authenticate("tester", "123456789"));
			Assert.IsNotNull(user);
			Assert.AreEqual("tester", user.Name);
		}

		[Test]
		public void Authenticate_Fail() {
			User user = null;

			Assert.Throws<SecurityException>(() => user = Database.Authenticate("test2", "12545587"));
			Assert.IsNull(user);
		}

		[Test]
		public void AdminChangesUserPassword() {
			Assert.DoesNotThrow(() => Query.Session.Access.AlterUserPassword("tester", "0123456789"));
		}

		[Test]
		public void SetUserRoles() {
			Assert.DoesNotThrow(() => Query.Session.Access.AddUserToRole("tester", "test_group"));
			Assert.DoesNotThrow(() => Query.Session.Access.AddUserToRole("tester", SystemRoles.UserManagerRole));

			User user = null;
			Assert.DoesNotThrow(() => user = Query.Session.Access.GetUser("tester"));
			Assert.IsNotNull(user);

			Role[] userRoles = null;
			Assert.DoesNotThrow(() => userRoles = Query.Session.Access.GetUserRoles(user.Name));
			Assert.IsNotNull(userRoles);

			var roleNames = userRoles.Select(x => x.Name).ToArray();
			Assert.Contains("test_group", roleNames);
			Assert.Contains(SystemRoles.UserManagerRole, roleNames);

			Assert.IsTrue(Query.Session.Access.UserIsInRole("tester", "test_group"));
		}

		[Test]
		public void LockUser() {
			Query.Session.Access.SetUserStatus("tester", UserStatus.Locked);

			UserStatus status = new UserStatus();
			Assert.DoesNotThrow(() => status = Query.Session.Access.GetUserStatus("tester"));
			Assert.AreEqual(UserStatus.Locked, status);
		}
	}
}
