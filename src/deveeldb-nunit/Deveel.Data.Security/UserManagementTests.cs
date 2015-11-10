using System;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UserManagementTests : ContextBasedTest {
		protected override IDatabase CreateDatabase(IDatabaseContext context) {
			var database = base.CreateDatabase(context);

			var testName = TestContext.CurrentContext.Test.Name;
			if (testName != "CreateUser") {
				using (var queryContext = database.CreateQueryContext(AdminUserName, AdminPassword)) {
					queryContext.CreateUser("tester", "123456789");
					queryContext.Commit();
				}
			}

			return database;
		}

		[Test]
		public void CreateUser() {
			User user = null;
			Assert.DoesNotThrow(() => user = QueryContext.CreateUser("tester", "123456"));
			Assert.IsNotNull(user);

			Assert.AreEqual("tester", user.Name);

			bool exists = false;
			Assert.DoesNotThrow(() => exists = QueryContext.UserExists("tester"));
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
			Assert.DoesNotThrow(() => exists = QueryContext.UserExists("tester"));
			Assert.IsTrue(exists);

			Assert.Throws<SecurityException>(() => QueryContext.CreateUser("tester", "123456789"));
		}

		[Test]
		public void DropUser() {
			Assert.DoesNotThrow(() => QueryContext.DeleteUser("tester"));

			bool exists = false;
			Assert.DoesNotThrow(() => QueryContext.UserExists("tester"));
			Assert.IsFalse(exists);
		}

		[Test]
		public void AdminChangesUserPassword() {
			Assert.DoesNotThrow(() => QueryContext.AlterUserPassword("tester", "0123456789"));
		}

		[Test]
		public void SetUserGroups() {
			Assert.DoesNotThrow(() => QueryContext.AddUserToGroup("tester", "test_group"));
			Assert.DoesNotThrow(() => QueryContext.AddUserToGroup("tester", SystemGroups.UserManagerGroup));

			User user = null;
			Assert.DoesNotThrow(() => user = QueryContext.GetUser("tester"));
			Assert.IsNotNull(user);

			string[] userGroups = null;
			Assert.DoesNotThrow(() => userGroups = user.Groups);
			Assert.IsNotNull(userGroups);
			Assert.Contains("test_group", userGroups);
			Assert.Contains(SystemGroups.UserManagerGroup, userGroups);

			Assert.IsTrue(QueryContext.UserBelongsToGroup("tester", "test_group"));
		}

		[Test]
		public void LockUser() {
			Assert.DoesNotThrow(() => QueryContext.SetUserStatus("tester", UserStatus.Locked));

			UserStatus status = new UserStatus();
			Assert.DoesNotThrow(() => status = QueryContext.GetUserStatus("tester"));
			Assert.AreEqual(UserStatus.Locked, status);
		}
	}
}
