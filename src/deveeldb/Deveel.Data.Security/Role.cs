using System;

namespace Deveel.Data.Security {
	public sealed class Role : Privileged {
		internal Role(ISession session, string name)
			: base(session, name) { 
		}

		public bool IsSystem {
			get { return SystemRoles.IsSystemRole(Name); }
		}
	}
}
