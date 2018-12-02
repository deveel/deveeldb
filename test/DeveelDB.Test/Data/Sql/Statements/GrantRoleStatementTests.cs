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
	public class GrantRoleStatementTests : IDisposable {
		private string grantedUser;
		private string grantedRole;

		private IContext adminContext;
		private IContext userContext;
		private IContext userInAdminRoleContext;

		public GrantRoleStatementTests() {
			var container = new ServiceContainer();

			var securityManager = new Mock<ISecurityManager>();
			securityManager.Setup(x =>
					x.AddUserToRoleAsync(It.IsNotNull<string>(), It.IsAny<string>()))
				.Callback<string, string>((x, y) => {
					grantedUser = x;
					grantedRole = y;
				})
				.Returns<string, string>((x, y) => Task.FromResult(true));

			securityManager.Setup(x => x.UserExistsAsync(It.IsAny<string>()))
				.Returns<string>(x => Task.FromResult(true));

			securityManager.Setup(x => x.RoleExistsAsync(It.IsAny<string>()))
				.Returns<string>(x => Task.FromResult(true));

			securityManager.Setup(x => x.IsUserInRoleAsync(It.IsNotNull<string>(), It.IsNotNull<string>()))
				.Returns<string, string>((x, y) => Task.FromResult(false));


			securityManager.Setup(x => x.GetUserRolesAsync(It.Is<string>(u => u == "user2")))
				.Returns<string>(x => Task.FromResult<IEnumerable<Role>>(new[] {new Role("admin_group")}));

			container.RegisterInstance<ISecurityManager>(securityManager.Object);

			var cache = new PrivilegesCache(null);
			cache.SetSystemPrivileges("admin_group", SqlPrivileges.Admin);

			container.RegisterInstance<IAccessController>(cache);

			var systemContext = new Mock<IContext>();
			systemContext.SetupGet(x => x.Scope)
				.Returns(container);

			adminContext = CreateUserSession(systemContext.Object, User.System);
			userContext = CreateUserSession(systemContext.Object, new User("user1"));
			userInAdminRoleContext = CreateUserSession(systemContext.Object, new User("user2"));
		}

		private static IContext CreateUserSession(IContext parent, User user) {
			var userSession = new Mock<ISession>();
			userSession.SetupGet(x => x.User)
				.Returns(user);
			userSession.SetupGet(x => x.Scope)
				.Returns(parent.Scope.OpenScope(KnownScopes.Session));
			userSession.SetupGet(x => x.ParentContext)
				.Returns(parent);

			return userSession.Object;
		}

		[Theory]
		[InlineData("anto", "role1")]
		public async void AdminGrantsRoleToUser(string user, string role) {
			var statement = new GrantRoleStatement(user, role);
			var result = await statement.ExecuteAsync(adminContext);

			Assert.Null(result);
			Assert.Equal(user, grantedUser);
			Assert.Equal(role, grantedRole);
		}

		[Theory]
		[InlineData("user2", "role1")]
		public async void UserCreatesOtherUser(string user, string role) {
			var statement = new GrantRoleStatement(user, role);

			await Assert.ThrowsAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(userContext));
		}

		[Theory]
		[InlineData("user2", "role1")]
		public async void UserInAdminRoleCreatesOtherUser(string user, string role) {
			var statement = new GrantRoleStatement(user, role);
			var result = await statement.ExecuteAsync(userInAdminRoleContext);


			Assert.Null(result);
			Assert.Equal(user, grantedUser);
			Assert.Equal(role, grantedRole);
		}

		[Theory]
		[InlineData("user2", "role1", "GRANT role1 TO user2")]
		public void GrantRole_AsString(string user, string role, string expected) {
			var statement = new GrantRoleStatement(user, role);

			Assert.Equal(expected, statement.ToSqlString());
		}

		public void Dispose() {
			adminContext?.Dispose();
			userContext?.Dispose();
			userInAdminRoleContext?.Dispose();
		}
	}
}