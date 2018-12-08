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
using System.Linq;
using System.Text;

using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Parsing;

using Xunit;

namespace Deveel.Data.Sql.Statements
{
	public class SqlSecurityStatementTests : IDisposable {
		private IContext context;
		private ISqlParser parser;

		public SqlSecurityStatementTests() {
			context = ContextUtil.NewParseContext();
			parser = context.Scope.Resolve<ISqlParser>();

		}

		[Theory]
		[InlineData("CREATE USER sa IDENTIFIED BY PASSWORD 'abc1234'", "sa", "abc1234")]
		public void ParseCreateUserWithPassword(string sql, string userName, string password) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<CreateUserStatement>(result.Statements.First());

			var statement = (CreateUserStatement) result.Statements.First();

			Assert.NotNull(statement.Location);
			Assert.Equal(userName, statement.UserName);
			Assert.IsType<PasswordIdentificationInfo>(statement.IdentificationInfo);
			Assert.Equal(password, ((PasswordIdentificationInfo) statement.IdentificationInfo).Password);
		}

		[Theory]
		[InlineData("DROP USER sa", "sa")]
		public void ParseDropUser(string sql, string userName) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<DropUserStatement>(result.Statements.First());

			var statement = (DropUserStatement) result.Statements.First();

			Assert.NotNull(statement.Location);
			Assert.Equal(userName, statement.UserName);
		}

		[Theory]
		[InlineData("GRANT ALL ON table1 TO user1", "user1", "table1", "ALL", false)]
		[InlineData("GRANT SELECT ON table2 TO user2 WITH GRANT OPTION", "user2", "table2", "SELECT", true)]
		[InlineData("GRANT INSERT, UPDATE, delete ON view1 TO role1", "role1", "view1", "INSERT, UPDATE, DELETE", false)]
		public void GrantObjectPrivileges(string sql, string grantee, string objName, string privs, bool withGrant) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<GrantObjectPrivilegesStatement>(result.Statements.First());

			var statement = (GrantObjectPrivilegesStatement) result.Statements.First();

			Assert.NotNull(statement.Location);
			Assert.NotNull(statement.ObjectName);
			Assert.Equal(objName, statement.ObjectName.ToString());
			Assert.Equal(grantee, statement.Grantee);
			Assert.Equal(privs, statement.Privileges.ToString(SqlPrivileges.Resolver));
			Assert.Equal(withGrant, statement.WithGrantOption);
		}

		[Theory]
		[InlineData("GRANT role1 TO user2", "role1", "user2")]
		public void GrantRoleToUser(string sql, string role, string user) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<GrantRoleStatement>(result.Statements.First());

			var statement = (GrantRoleStatement) result.Statements.First();

			Assert.Equal(role, statement.RoleName);
			Assert.Equal(user, statement.UserName);
		}

		[Theory]
		[InlineData("REVOKE role1 FROM user1", "role1", "user1")]
		public void RevokeRoleFromUser(string sql, string role, string user) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<RevokeRoleStatement>(result.Statements.First());

			var statement = (RevokeRoleStatement) result.Statements.First();

			Assert.Equal(role, statement.RoleName);
			Assert.Equal(user, statement.UserName);
		}

		[Theory]
		[InlineData("DROP ROLE role1", "role1")]
		public void DropRole(string sql, string role) {
			var result = parser.Parse(context, sql);

			Assert.NotNull(result);
			Assert.False(result.Failed);
			Assert.True(result.Succeeded);
			Assert.Empty(result.Messages);
			Assert.Single(result.Statements);

			Assert.IsType<DropRoleStatement>(result.Statements.First());

			var statement = (DropRoleStatement) result.Statements.First();

			Assert.NotNull(statement.Location);
			Assert.Equal(role, statement.RoleName);

		}

		public void Dispose() {
			context?.Dispose();
		}
	}
}
