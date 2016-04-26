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
	public sealed class DropUserTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateUser("tester", "12345");
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DeleteUser("tester");
			return true;
		}

		protected override void OnBeforeTearDown(string testName) {
			if (testName == "Existing")
				base.OnBeforeTearDown(testName);
		}

		[Test]
		public void Existing() {
			Query.DropUser("tester");

			var exists = Query.Session.Access().UserExists("tester");
			Assert.IsFalse(exists);
		}

		[Test]
		public void PublicUser() {
			Assert.Throws<SecurityException>(() => Query.DropUser(User.PublicName));
		}

		[Test]
		public void SystemUser() {
			Assert.Throws<SecurityException>(() => Query.DropUser(User.SystemName));
		}
	}
}
