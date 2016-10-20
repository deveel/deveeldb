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

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class Role : Privileged {
		internal Role(ISession session, string name)
			: base(session, name) { 
		}

		//public bool IsSystem {
		//	get { return SystemRoles.IsSystemRole(Name); }
		//}

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
