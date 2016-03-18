using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class Role : Privileged {
		internal Role(ISession session, string name)
			: base(session, name) { 
		}

		public bool IsSystem {
			get { return SystemRoles.IsSystemRole(Name); }
		}

		public bool IsSecureAccess {
			get { return String.Equals(Name, SystemRoles.SecureAccessRole); }
		}

		public bool IsUserManager {
			get { return String.Equals(Name, SystemRoles.UserManagerRole); }
		}

		public bool IsSchemaManager {
			get { return String.Equals(Name, SystemRoles.SchemaManagerRole); }
		}

		public override bool CanManageUsers() {
			return IsSecureAccess ||
			       IsUserManager;
		}

		public override bool CanManageSchema() {
			return IsSecureAccess ||
			       IsSchemaManager;
		}

		public override bool HasPrivileges(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSecureAccess)
				return true;

			return base.HasPrivileges(objectType, objectName, privileges);
		}

		public override bool HasGrantOption(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSecureAccess)
				return true;

			return base.HasGrantOption(objectType, objectName, privileges);
		}
	}
}
