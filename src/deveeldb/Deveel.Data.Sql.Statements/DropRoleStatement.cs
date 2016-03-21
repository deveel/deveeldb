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
