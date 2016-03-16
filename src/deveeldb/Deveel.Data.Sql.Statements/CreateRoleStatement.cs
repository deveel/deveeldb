using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateRoleStatement : SqlStatement {
		public CreateRoleStatement(string roleName) {
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");

			RoleName = roleName;
		}

		public string RoleName { get; private set; }
	}
}
