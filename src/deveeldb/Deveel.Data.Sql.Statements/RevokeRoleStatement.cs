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
using System.Runtime.Remoting.Channels;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class RevokeRoleStatement : SqlStatement {
		public RevokeRoleStatement(string grantee, string roleName) {
			if (String.IsNullOrEmpty(grantee))
				throw new ArgumentNullException("grantee");
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");

			Grantee = grantee;
			RoleName = roleName;
		}

		public string Grantee { get; private set; }

		public string RoleName { get; private set; }

		public bool Admin { get; set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.RoleExists(RoleName))
				throw new StatementException(String.Format("The role '{0}' does not exist", RoleName));
			if (!context.DirectAccess.UserExists(Grantee) &&
				!context.DirectAccess.RoleExists(Grantee))
				throw new StatementException(String.Format("User or role '{0}' does not exist.", Grantee));

			if (!context.User.CanRevokeRole(RoleName))
				throw new SecurityException(String.Format("User '{0}' has no role rights to revoke role '{1}' from '{2'}'.",
					context.User.Name, RoleName, Grantee));

			if (Admin) {
				// TODO:
				throw new NotImplementedException();
			}

			context.DirectAccess.RemoveUserFromRole(Grantee, RoleName);
		}
	}
}
