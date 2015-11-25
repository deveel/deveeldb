using System;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UserManagementTests : ContextBasedTest {
		protected override ISession CreateAdminSession(IDatabase database) {
			var testName = TestContext.CurrentContext.Test.Name;
			if (testName != "CreateUser") {
				using (var session = base.CreateAdminSession(database)) {
					using (var query = session.CreateQuery()) {
						query.CreateUser("tester", "123456789");
						query.Commit();
					}
				}
			}
			return base.CreateAdminSession(database);
		}

		[Test]
		public void CreateUser() {
			User user = null;
			Assert.DoesNotThrow(() => user = Query.CreateUser("tester", "123456"));
			Assert.IsNotNull(user);

			Assert.AreEqual("tester", user.Name);

			bool exists = false;
			Assert.DoesNotThrow(() => exists = Query.UserExists("tester"));
			Assert.IsTrue(exists);
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
		public void CreateExistingUser() {
			bool exists = false;
			Assert.DoesNotThrow(() => exists = Query.UserExists("tester"));
			Assert.IsTrue(exists);

			Assert.Throws<SecurityException>(() => Query.CreateUser("tester", "123456789"));
		}

		[Test]
		public void DropUser() {
			Assert.DoesNotThrow(() => Query.DeleteUser("tester"));

			bool exists = false;
			Assert.DoesNotThrow(() => Query.UserExists("tester"));
			Assert.IsFalse(exists);
		}

		[Test]
		public void AdminChangesUserPassword() {
			Assert.DoesNotThrow(() => Query.AlterUserPassword("tester", "0123456789"));
		}

		[Test]
		public void SetUserGroups() {
			Assert.DoesNotThrow(() => Query.AddUserToGroup("tester", "test_group"));
			Assert.DoesNotThrow(() => Query.AddUserToGroup("tester", SystemGroups.UserManagerGroup));

			User user = null;
			Assert.DoesNotThrow(() => user = Query.GetUser("tester"));
			Assert.IsNotNull(user);

			string[] userGroups = null;
			Assert.DoesNotThrow(() => userGroups = Query.GetGroupsUserBelongsTo(user.Name));
			Assert.IsNotNull(userGroups);
			Assert.Contains("test_group", userGroups);
			Assert.Contains(SystemGroups.UserManagerGroup, userGroups);

			Assert.IsTrue(Query.UserBelongsToGroup("tester", "test_group"));
		}

		[Test]
		public void LockUser() {
			Query.SetUserStatus("tester", UserStatus.Locked);

			UserStatus status = new UserStatus();
			Assert.DoesNotThrow(() => status = Query.GetUserStatus("tester"));
			Assert.AreEqual(UserStatus.Locked, status);
		}
	}
}
