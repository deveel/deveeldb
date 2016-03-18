// 
//  Copyright 2010-2015 Deveel
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
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	/// <summary>
	/// Provides the information for a user in a database system
	/// </summary>
	public sealed class User : Privileged {
		internal User(ISession session, string name)
			: base(session, name) { 
		}

		/// <summary>
		/// The name of the <c>PUBLIC</c> special user.
		/// </summary>
		public const string PublicName = "PUBLIC";

		/// <summary>
		/// The name of the <c>SYSTEM</c> special user.
		/// </summary>
		public const string SystemName = "@SYSTEM";

		/// <summary>
		/// Gets a boolean value indicating if this user represents the
		/// <c>SYSTEM</c> special user.
		/// </summary>
		/// <seealso cref="SystemName"/>
		public bool IsSystem {
			get { return Name.Equals(SystemName); }
		}

		/// <summary>
		/// Gets a boolean value indicating if this user represents the
		/// <c>PUBLIC</c> special user.
		/// </summary>
		public bool IsPublic {
			get { return Name.Equals(PublicName); }
		}

		public UserStatus Status {
			get {
				AssertInContext();
				return Session.Access.GetUserStatus(Name);
			}
		}

		public Role[] Roles {
			get {
				AssertInContext();
				return Session.Access.GetUserRoles(Name);
			}
		}

		public bool IsRoleAdmin(string roleName) {
			AssertInContext();
			return Session.Access.UserIsRoleAdmin(Name, roleName);
		}

		public bool IsInRole(string roleName) {
			AssertInContext();
			return Session.Access.UserIsInRole(Name, roleName);
		}

		public bool CanGrantRole(string roleName) {
			return IsSystem ||
			       IsInRole(SystemRoles.SecureAccessRole) ||
			       IsRoleAdmin(roleName);
		}

		public bool CanManageRoles() {
			return IsSystem ||
			       IsInRole(SystemRoles.SecureAccessRole);
		}

		public override bool CanManageUsers() {
			if (IsSystem)
				return true;

			var roles = Roles;
			if (roles == null || roles.Length == 0)
				return false;

			return roles.Any(role => role.CanManageUsers());
		}

		public bool CanDropUser(string userName) {
			if (String.Equals(Name, userName, StringComparison.OrdinalIgnoreCase))
				return true;

			return CanManageUsers();
		}

		public override bool CanManageSchema() {
			if (IsSystem)
				return true;

			var roles = Roles;
			if (roles == null || roles.Length == 0)
				return false;

			return roles.Any(role => role.CanManageSchema());
		}

		public override bool HasPrivileges(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSystem ||
				IsInRole(SystemRoles.SecureAccessRole))
				return true;

			if (base.HasPrivileges(objectType, objectName, privileges))
				return true;

			var roles = Roles;
			if (roles == null || roles.Length == 0)
				return false;

			return roles.Any(role => role.HasPrivileges(objectType, objectName, privileges));
		}

		public override bool HasGrantOption(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSystem)
				return true;

			if (base.HasGrantOption(objectType, objectName, privileges))
				return true;

			var roles = Roles;
			if (roles == null || roles.Length == 0)
				return false;

			return roles.Any(role => role.HasGrantOption(objectType, objectName, privileges));
		}

		public static bool IsSystemUserName(string name) {
			return String.Equals(SystemName, name, StringComparison.OrdinalIgnoreCase);
		}
	}
}