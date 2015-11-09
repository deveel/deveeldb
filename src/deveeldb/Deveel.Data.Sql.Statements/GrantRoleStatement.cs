using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Statements {
	public sealed class GrantRoleStatement : SqlStatement {
		public GrantRoleStatement(string userName, string role) 
			: this(userName, role, false) {
		}

		public GrantRoleStatement(string userName, string role, bool withAdmin) {
			UserName = userName;
			Role = role;
			WithAdmin = withAdmin;
		}

		public string Role { get; private set; }

		public string UserName { get; private set; }

		public bool WithAdmin { get; private set; }
	}
}
