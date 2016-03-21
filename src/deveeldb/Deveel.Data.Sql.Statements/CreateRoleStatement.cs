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
