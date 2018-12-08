// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public static class ContextExtensions {
		public static User User(this IContext context) {
			var current = context;
			while (current != null) {
				if (current is ISession)
					return ((ISession) current).User;

				current = current.ParentContext;
			}

			return null;
		}

		public static async Task<bool> UserHasPrivileges(this IContext context, ObjectName objectName, Privilege privilege) {
			var user = context.User();
			if (user == null)
				return false;

			// if no security resolver was registered this means no security
			// checks are required
			var resolver = context.GetService<IAccessController>();
			if (resolver == null)
				return false;

			if (!await resolver.HasObjectPrivilegesAsync(user.Name, objectName, privilege)) {
				var roleManager = context.GetService<IRoleManager>();
				if (roleManager == null)
					return false;

				var roles = await roleManager.GetUserRolesAsync(user.Name);
				foreach (var role in roles) {
					if (await resolver.HasObjectPrivilegesAsync(role.Name, objectName, privilege))
						return true;
				}

				return false;
			}

			return true;
		}

		public static Task<bool> UserCanCreateInSchema(this IContext context, string schemaName) {
			return context.UserHasPrivileges(new ObjectName(schemaName), SqlPrivileges.Create);
		}

		public static Task<bool> UserCanSelectFrom(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(tableName, SqlPrivileges.Select);

		public static Task<bool> UserCanUpdate(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(tableName, SqlPrivileges.Update);

		public static Task<bool> UserCanDelete(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(tableName, SqlPrivileges.Delete);

		public static Task<bool> UserCanInsert(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(tableName, SqlPrivileges.Insert);

		public static Task<bool> UserCanExecute(this IContext context, ObjectName methodName)
			=> context.UserHasPrivileges(methodName, SqlPrivileges.Execute);

		public static Task<bool> UserCanAlter(this IContext context, ObjectName objectName)
			=> context.UserHasPrivileges(objectName, SqlPrivileges.Alter);

		public static Task<bool> UserCanReference(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(tableName, SqlPrivileges.References);

		public static async Task<bool> UserHasSystemPrivilege(this IContext context, Privilege privilege) {
			var user = context.User();
			if (user == null)
				return false;

			// if no security resolver was registered this means no security
			// checks are required
			var resolver = context.GetService<IAccessController>();
			if (resolver == null)
				return false;

			if (!await resolver.HasSystemPrivilegesAsync(user.Name, privilege)) {
				var roleManager = context.GetService<IRoleManager>();
				if (roleManager == null)
					return false;

				var roles = await roleManager.GetUserRolesAsync(user.Name);
				foreach (var role in roles) {
					if (await resolver.HasSystemPrivilegesAsync(role.Name, privilege))
						return true;
				}

				return false;
			}

			return true;
		}

		public static async Task<bool> UserIsAdmin(this IContext context) {
			var user = context.User();
			if (user.IsSystem)
				return true;

			return await context.UserHasSystemPrivilege(SqlPrivileges.Admin);
		}

		public static async Task<bool> UserCanGrant(this IContext context, ObjectName objName, Privilege privileges, bool withOption) {
			if (await context.UserIsAdmin())
				return true;

			var grantManager = context.GetGrantManager();
			var userGrants = await grantManager.GetGrantsAsync(context.User().Name);

			foreach (var grant in userGrants.Where(x => x.ObjectName.Equals(objName, true))) {
				if  (grant.Privileges.Permits(privileges)) {
					return !withOption || grant.WithOption;
				}
			}

			return false;
		}

		public static IRoleManager GetRoleManager(this IContext context) {
			var roleManager = context.GetService<IRoleManager>();
			if (roleManager == null)
				throw new SystemException("There is no security manager defined in the system");

			return roleManager;
		}

		public static IUserManager GetUserManager(this IContext context) {
			var userManager = context.GetService<IUserManager>();
			if (userManager == null)
				throw new SystemException("There is no user manager defined in the system");

			return userManager;
		}

		public static IGrantManager GetGrantManager(this IContext context) {
			var grantManager = context.GetService<IGrantManager>();
			if (grantManager == null)
				throw new SystemException("There is no grant manager defined in the system");

			return grantManager;
		}
	}
}