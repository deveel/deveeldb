using System;

using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UserManagementTests : ContextBasedTest {
		protected override void OnSetUp() {
			var currentTest = TestContext.CurrentContext.Test;
			if (currentTest.Name != "CreateUser") {
				QueryContext.CreateUser("tester", "123456789");
			}

			base.OnSetUp();
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
			Assert.DoesNotThrow(() => QueryContext.AddUserToGroup("tester", SystemGroupNames.UserManagerGroup));

			User user = null;
			Assert.DoesNotThrow(() => user = QueryContext.GetUser("tester"));
			Assert.IsNotNull(user);

			string[] userGroups = null;
			Assert.DoesNotThrow(() => userGroups = user.Groups);
			Assert.IsNotNull(userGroups);
			Assert.Contains("test_group", userGroups);
			Assert.Contains(SystemGroupNames.UserManagerGroup, userGroups);
		}

		[Test]
		public void LockUser() {
			Assert.DoesNotThrow(() => QueryContext.SetUserStatus("tester", UserStatus.Locked));

		}
	}
}
