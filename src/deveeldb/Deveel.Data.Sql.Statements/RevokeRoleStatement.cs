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

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.RoleExists(RoleName))
				throw new StatementException(String.Format("The role '{0}' does not exist", RoleName));
			if (!context.DirectAccess.UserExists(Grantee) &&
				!context.DirectAccess.RoleExists(Grantee))
				throw new StatementException(String.Format("User or role '{0}' does not exist.", Grantee));

			if (!context.DirectAccess.UserIsRoleAdmin(context.Request.UserName(), RoleName))
				throw new SecurityException(String.Format("User '{0}' has no role administration rights for '{1}'.",
					context.User.Name, RoleName));

			context.DirectAccess.RemoveUserFromRole(Grantee, RoleName);
		}
	}
}
