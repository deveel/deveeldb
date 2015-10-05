using System;

using Deveel.Data.Protocol;
using Deveel.Data.Security;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class CreateUserTests : ContextBasedTest {
		[Test]
		public void Create() {
			User user = null;

		 	Assert.DoesNotThrow(() => user = QueryContext.CreateUser("test", "abc1234"));
			Assert.IsNotNull(user);

			// Commit the change
			Assert.DoesNotThrow(() => QueryContext.Commit());

			// at this point the context became invalid (after the commit) and we need a new one
			bool userExists = false;
			using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					Assert.DoesNotThrow(() => userExists = context.UserExists("test"));
				}
			}
			
			Assert.IsTrue(userExists);
		}

		[Test]
		public void CreateAndAuthenticate() {
			User user = null;

			Assert.DoesNotThrow(() => user = QueryContext.CreateUser("test", "abc1234"));

			// We need to grant access to local if we want to authenticate
			Assert.DoesNotThrow(() => QueryContext.GrantHostAccessToUser("test", KnownConnectionProtocols.Local, "%"));
			Assert.IsNotNull(user);

			Assert.AreEqual("test", user.Name);

			Assert.DoesNotThrow(() => QueryContext.Session.Commit());

			Assert.DoesNotThrow(() => user = Database.Authenticate("test", "abc1234"));
			Assert.IsNotNull(user);
			Assert.AreEqual("test", user.Name);
		}
	}
}
