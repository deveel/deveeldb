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

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CreateUserTests : ContextBasedTest {
		[Test]
		public void WithSimplePassword() {
			const string userName = "tester";

			Query.CreateUser(userName, "12345");

			var exists = Query.Session.Access().UserExists(userName);
			Assert.IsTrue(exists);
		}

		[Test]
		public void ExistingUser() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(AdminUserName, "0123456789"));
		}

		[Test]
		public void PublicUser() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(User.PublicName, "12345"));
		}

		[Test]
		public void WithRoleName() {
			Assert.Throws<SecurityException>(() => Query.CreateUser(SystemRoles.LockedRole, "0123456789"));
		}
	}
}
