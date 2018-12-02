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
using System.Text;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Services;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public class GrantObjectPrivilegesStatementTests {
		private IContext adminContext;
		private IContext userContext;
		private IContext userInAdminRoleContext;

		private Grant grant;

		public GrantObjectPrivilegesStatementTests() {
			var container = new ServiceContainer();

			var securityManager = new Mock<ISecurityManager>();
			securityManager.Setup(x => x.GrantToUserAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsNotNull<ObjectName>(),
					It.IsAny<Privilege>(), It.IsAny<bool>()))
				.Callback<string, string, ObjectName, Privilege, bool>((granter, grantee, obj, priv, option) =>
					grant = new Grant(granter, grantee, obj, priv, option))
				.Returns<string, string, ObjectName, Privilege, bool>((a, b, c, d, e) => Task.FromResult(true));

			securityManager.Setup(x => x.UserExistsAsync(It.IsNotNull<string>()))
				.Returns<string>(x => Task.FromResult(true));

			securityManager.Setup(x => x.GetUserRolesAsync(It.Is<string>(u => u == "user2")))
				.Returns<string>(x => Task.FromResult<IEnumerable<Role>>(new[] {new Role("admin_group")}));

			container.RegisterInstance<ISecurityManager>(securityManager.Object);

			var cache = new PrivilegesCache(null);
			cache.SetSystemPrivileges("admin_group", SqlPrivileges.Admin);

			container.RegisterInstance<IAccessController>(cache);

			var objManager = new Mock<IDbObjectManager>();
			objManager.Setup(x => x.ObjectExistsAsync(It.IsNotNull<ObjectName>()))
				.Returns<ObjectName>(x => Task.FromResult(true));

			container.RegisterInstance<IDbObjectManager>(objManager.Object);

			var systemContext = new Mock<IContext>();
			systemContext.SetupGet(x => x.Scope)
				.Returns(container);

			adminContext = MockedSession.Create(systemContext.Object, User.System);
			userContext = MockedSession.Create(systemContext.Object, new User("user1"));
			userInAdminRoleContext = MockedSession.Create(systemContext.Object, new User("user2"));
		}

		[Theory]
		[InlineData("user10", "sys.table1", "SELECT", true)]
		public async void AdminGrantsToUser(string user, string objName, string privs, bool withOption) {
			var statement = new GrantObjectPrivilegesStatement(user, SqlPrivileges.Resolver.ResolvePrivilege(privs),
				ObjectName.Parse(objName), withOption, new string[0]);
			var result = await statement.ExecuteAsync(adminContext);

			Assert.Null(result);
			Assert.Equal(adminContext.User().Name, grant.Granter);
			Assert.Equal(user, grant.Grantee);
			Assert.Equal(objName, grant.ObjectName.FullName);
			Assert.Equal(privs, grant.Privileges.ToString(SqlPrivileges.Resolver));
			Assert.Equal(withOption, grant.WithOption);
		}

		[Theory]
		[InlineData("user10", "sys.table1", "SELECT", true)]
		public async void UserGrantsToOtherUser(string user, string objName, string privs, bool withOption) {
			var statement = new GrantObjectPrivilegesStatement(user, SqlPrivileges.Resolver.ResolvePrivilege(privs),
				ObjectName.Parse(objName), withOption, new string[0]);

			await Assert.ThrowsAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(userContext));
		}

		[Theory]
		[InlineData("user10", "sys.table1", "SELECT", true)]
		public async void UserInAdminRoleGrantsToUser(string user, string objName, string privs, bool withOption) {
			var statement = new GrantObjectPrivilegesStatement(user, SqlPrivileges.Resolver.ResolvePrivilege(privs),
				ObjectName.Parse(objName), withOption, new string[0]);
			var result = await statement.ExecuteAsync(userInAdminRoleContext);

			Assert.Null(result);
			Assert.Equal(userInAdminRoleContext.User().Name, grant.Granter);
			Assert.Equal(user, grant.Grantee);
			Assert.Equal(objName, grant.ObjectName.FullName);
			Assert.Equal(privs, grant.Privileges.ToString(SqlPrivileges.Resolver));
			Assert.Equal(withOption, grant.WithOption);
		}

	}
}