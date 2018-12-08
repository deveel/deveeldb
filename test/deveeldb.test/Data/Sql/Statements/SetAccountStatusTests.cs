// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Services;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetAccountStatusTests : IDisposable {
		private IContext adminContext;
		private IContext userContext;
		private IContext userInAdminRoleContext;

		private string userSet;
		private UserStatus newStatus;

		public SetAccountStatusTests() {
			var container = new ServiceContainer();

			var userManager = new Mock<IUserManager>();
			userManager.Setup(x => x.GetUserStatusAsync(It.IsAny<string>()))
				.Returns<string>(x => Task.FromResult(UserStatus.Unlocked));
			userManager.Setup(x => x.UserExistsAsync(It.IsNotNull<string>()))
				.Returns(Task.FromResult(true));
			userManager.Setup(x => x.SetUserStatusAsync(It.IsNotNull<string>(), It.IsAny<UserStatus>()))
				.Callback<string, UserStatus>((x, y) => {
					userSet = x;
					newStatus = y;
				})
				.Returns(Task.FromResult(true));


			var securityManager = new Mock<IRoleManager>();
			securityManager.Setup(x => x.GetUserRolesAsync(It.Is<string>(u => u == "user2")))
				.Returns<string>(x => Task.FromResult<IEnumerable<Role>>(new[] {new Role("admin_group")}));

			container.RegisterInstance<IRoleManager>(securityManager.Object);
			container.RegisterInstance<IUserManager>(userManager.Object);

			var cache = new PrivilegesCache(null);
			cache.SetSystemPrivileges("admin_group", SqlPrivileges.Admin);

			container.RegisterInstance<IAccessController>(cache);

			var systemContext = new Mock<IContext>();
			systemContext.SetupGet(x => x.Scope)
				.Returns(container);

			adminContext = MockedSession.Create(systemContext.Object, User.System);
			userContext = MockedSession.Create(systemContext.Object, new User("user1"));
			userInAdminRoleContext = MockedSession.Create(systemContext.Object, new User("user2"));
		}

		[Theory]
		[InlineData("user1", UserStatus.Unlocked)]
		public async void AdminSetsAccountStatus(string userName, UserStatus status) {
			var statement = new AlterUserStatement(userName, new SetAccountStatusAction(status));
			var result = await statement.ExecuteAsync(adminContext);

			Assert.Null(result);
			Assert.Equal(userName, userSet);
			Assert.Equal(status, newStatus);
		}

		[Theory]
		[InlineData("user1", UserStatus.Unlocked)]
		public async void UserSetsAccountStatus(string userName, UserStatus status) {
			var statement = new AlterUserStatement(userName, new SetAccountStatusAction(status));

			await Assert.ThrowsAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(userContext));
		}

		[Theory]
		[InlineData("user1", UserStatus.Locked)]
		public async void UserInAdminRoleSetsAccountStatus(string userName, UserStatus status) {
			var statement = new AlterUserStatement(userName, new SetAccountStatusAction(status));
			var result = await statement.ExecuteAsync(userInAdminRoleContext);


			Assert.Null(result);
			Assert.Equal(userName, userSet);
			Assert.Equal(status, newStatus);
		}

		[Theory]
		[InlineData("user1", UserStatus.Locked, "ALTER USER user1 SET ACCOUNT STATUS LOCKED")]
		[InlineData("user32", UserStatus.Unlocked, "ALTER USER user32 SET ACCOUNT STATUS UNLOCKED")]
		public void SetAccountStatus_ToString(string userName, UserStatus status, string expected) {
			var statement = new AlterUserStatement(userName, new SetAccountStatusAction(status));

			Assert.Equal(expected, statement.ToSqlString());
		}

		public void Dispose() {
			adminContext?.Dispose();
			userContext?.Dispose();
			userInAdminRoleContext?.Dispose();
		}
	}
}