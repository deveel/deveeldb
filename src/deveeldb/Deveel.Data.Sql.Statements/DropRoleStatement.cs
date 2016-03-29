// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropRoleStatement : SqlStatement {
		public DropRoleStatement(string roleName) {
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");

			RoleName = roleName;
		}

		public string RoleName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {			
			if (SystemRoles.IsSystemRole(RoleName))
				throw new SecurityException(String.Format("The role '{0}' is system protected.", RoleName));

			if (!context.DirectAccess.RoleExists(RoleName))
				throw new StatementException(String.Format("The role '{0}' does not exists.", RoleName));

			if (!context.User.CanDropRole(RoleName))
				throw new SecurityException(String.Format("User '{0}' has not enough rights to drop a role.", context.User.Name));

			if (!context.DirectAccess.DropRole(RoleName))
				throw new StatementException(String.Format("The role '{0}' could not be deleted: maybe not found.", RoleName));
		}
	}
}
