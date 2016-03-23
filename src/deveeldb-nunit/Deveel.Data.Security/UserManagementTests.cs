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

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UserManagementTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			if (testName == "Authenticate_Success") {
				using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
					using (var query = session.CreateQuery()) {
						query.Access.CreateUser("tester", "123456789");
						session.Commit();
					}
				}
			}
		}

		[Test]
		public void Authenticate_Success() {
			bool authenticated = false;

			Assert.DoesNotThrow(() => authenticated = Database.Authenticate("tester", "123456789"));
			Assert.IsTrue(authenticated);
		}

		[Test]
		public void Authenticate_Fail() {
			bool authenticated = false;

			Assert.Throws<SecurityException>(() => authenticated = Database.Authenticate("test2", "12545587"));
			Assert.IsFalse(authenticated);
		}
	}
}
