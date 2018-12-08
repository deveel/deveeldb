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
	public sealed class CreateRoleStatement : SqlStatement {
		public CreateRoleStatement(string roleName) {
			RoleName = roleName ?? throw new ArgumentNullException(nameof(roleName));
		}

		public string RoleName { get; }

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			var securityManager = context.GetService<IRoleManager>();
			if (securityManager == null)
				throw new SystemException("There is no security manager defined in the system");

			if (await securityManager.RoleExistsAsync(RoleName))
				throw new SqlStatementException($"The role '{RoleName}' already exists.");

			if (!await securityManager.CreateRoleAsync(RoleName))
				throw new SqlStatementException($"It was not possible to create the role '{RoleName}' because of a system error.");

			context.RaiseEvent(new RoleCreatedEvent(this, RoleName));
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("CREATE ROLE ");
			builder.Append(RoleName);
		}
	}
}