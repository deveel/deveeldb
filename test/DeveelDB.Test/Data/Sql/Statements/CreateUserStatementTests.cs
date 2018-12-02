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
	public class CreateUserStatementTests : IDisposable {
		private string createdUser;

		private IContext adminContext;
		private IContext userContext;
		private IContext userInAdminRoleContext;

		public CreateUserStatementTests() {
			var container = new ServiceContainer();

			var securityManager = new Mock<ISecurityManager>();
			securityManager.Setup(x =>
					x.CreateUserAsync(It.IsNotNull<string>(), It.IsNotNull<IUserIdentificationInfo>()))
				.Callback<string, IUserIdentificationInfo>((x, y) => createdUser = x)
				.Returns<string, IUserIdentificationInfo>((x, y) => Task.FromResult(true));
			securityManager.Setup(x => x.GetUserRolesAsync(It.Is<string>(u => u == "user2")))
				.Returns<string>(x => Task.FromResult<IEnumerable<Role>>(new[] {new Role("admin_group")}));

			container.RegisterInstance<ISecurityManager>(securityManager.Object);

			var cache = new PrivilegesCache();
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
		[InlineData("anto", "1234")]
		public async void AdminCreatesUserWithPassword(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));
			var result = await statement.ExecuteAsync(adminContext);

			Assert.Null(result);
			Assert.Equal(user, createdUser);
		}

		[Theory]
		[InlineData("user2", "1234")]
		public async void UserCreatesOtherUser(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));

			await Assert.ThrowsAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(userContext));
		}

		[Theory]
		[InlineData("user2", "1234")]
		public async void UserInAdminRoleCreatesOtherUser(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));
			var result = await statement.ExecuteAsync(userInAdminRoleContext);


			Assert.Null(result);
			Assert.Equal(user, createdUser);
		}

		[Theory]
		[InlineData("@system", "abc1234")]
		[InlineData("@SYSTEM", "1234")]
		[InlineData("PUBLIC", "2345")]
		public async void CreateWithInvalidName(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));
			await Assert.ThrowsAsync<SqlStatementException>(() => statement.ExecuteAsync(adminContext));
		}

		[Theory]
		[InlineData("antonello", "abc1234", "CREATE USER antonello IDENTIFIED BY <password>")]
		public void CreateUserWithPassword_AsString(string userName, string password, string expected) {
			var statement = new CreateUserStatement(userName, new PasswordIdentificationInfo(password));

			var sql = statement.ToSqlString();
			Assert.Equal(expected, sql);
		}

		public void Dispose() {
			adminContext?.Dispose();
			userContext?.Dispose();
			userInAdminRoleContext?.Dispose();
		}
	}
}