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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropUserStatement : SqlStatement {
		public DropUserStatement(string userName) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
		}

		public string UserName { get; }

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		private async Task<bool> RevokeAllGrantedPrivileges(IContext context, IUserManager userManager, IRoleManager roleManager) {
			var user = context.User();
			var grantManager = context.GetGrantManager();

			var grants = await grantManager.GetGrantedAsync(user.Name);

			foreach (var grant in grants) {
				if (await roleManager.RoleExistsAsync(grant.Grantee)) {
					if (!await grantManager.RevokeFromRoleAsync(grant.Grantee, grant.ObjectName, grant.Privileges))
						return false;
				} else if (await userManager.UserExistsAsync(grant.Grantee)) {
					if (!await grantManager.RevokeFromUserAsync(user.Name, grant.Grantee, grant.ObjectName, grant.Privileges, grant.WithOption))
						return false;
				} else {
					// TODO: log this error but not throw
				}

				context.RaiseEvent(new ObjectPrivilegesRevokedEvent(this, user.Name, grant.Grantee, grant.ObjectName, grant.Privileges));
			}

			return true;
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			var userManager = context.GetUserManager();
			var roleManager = context.GetRoleManager();

			if (!await userManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"A user named '{UserName}' does not exist.");

			if (!await RevokeAllGrantedPrivileges(context, userManager, roleManager))
				throw new SqlStatementException($"It was not possible to revoke the privileges granted by '{UserName}'.");

			if (!await userManager.DropUserAsync(UserName))
				throw new SqlStatementException($"It was not possible to delete the user '{UserName}' because of a system error");

			context.RaiseEvent(new UserDroppedEvent(this, UserName));
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("DROP USER ");
			builder.Append(UserName);
		}
	}
}