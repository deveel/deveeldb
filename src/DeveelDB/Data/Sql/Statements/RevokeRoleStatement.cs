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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class RevokeRoleStatement : SqlStatement {
		public RevokeRoleStatement(string userName, string roleName) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
			RoleName = roleName ?? throw new ArgumentNullException(nameof(roleName));
		}

		public string UserName { get; }

		public string RoleName { get; }

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			var roleManager = context.GetRoleManager();
			var userManager = context.GetUserManager();

			if (!await roleManager.RoleExistsAsync(RoleName))
				throw new SqlStatementException($"The role '{RoleName}' does not exist.");
			if (!await userManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"The user '{UserName}' does not exist");

			if (!await roleManager.IsUserInRoleAsync(UserName, RoleName))
				throw new SqlStatementException($"The user '{UserName}' is not in the role '{RoleName}'");

			if (!await roleManager.RemoveUserFromRoleAsync(UserName, RoleName))
				throw new SqlStatementException($"It was not possible to revoke the role '{RoleName}' from user '{UserName}'");

			context.RaiseEvent<RoleRevokedEvent>(UserName, RoleName);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("REVOKE ");
			builder.Append(RoleName);
			builder.Append(" FROM ");
			builder.Append(UserName);
		}
	}
}