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
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class GrantRoleStatement : SqlStatement {
		public GrantRoleStatement(string userName, string roleName) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
			RoleName = roleName ?? throw new ArgumentNullException(nameof(roleName));
		}

		public string UserName { get; }

		public string RoleName { get; }

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			var securityManager = context.GetService<ISecurityManager>();
			if (securityManager == null)
				throw new SystemException("There is no security manager defined in the system");

			if (!await securityManager.RoleExistsAsync(RoleName))
				throw new SqlStatementException($"The role '{RoleName}' does not exist.");
			if (!await securityManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"The user '{UserName}' does not exist");

			if (await securityManager.IsUserInRoleAsync(UserName, RoleName))
				throw new SqlStatementException($"User '{UserName}' is already in role '{RoleName}'");

			if (!await securityManager.AddUserToRoleAsync(UserName, RoleName))
				throw new SqlStatementException($"It was not possible to assign the role '{RoleName}' to "+
				                                $"user '{UserName}' because of a system error");

			context.RaiseEvent(new RoleGrantedEvent(this, UserName, RoleName));
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("GRANT ");
			builder.Append(RoleName);
			builder.Append(" TO ");
			builder.Append(UserName);
		}
	}
}
