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
	public sealed class CreateRoleStatement : SqlStatement {
		public CreateRoleStatement(string roleName) {
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");

			RoleName = roleName;
		}

		public string RoleName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanManageRoles())
				throw new SecurityException(String.Format("User '{0}' has not enough rights to create roles.", context.User.Name));

			if (context.DirectAccess.RoleExists(RoleName))
				throw new InvalidOperationException(String.Format("Role '{0}' already exists.", RoleName));

			context.DirectAccess.CreateRole(RoleName);
			context.DirectAccess.AddUserToRole(context.User.Name, RoleName, true);
		}
	}
}
