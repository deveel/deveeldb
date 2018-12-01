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

		public static async Task<bool> UserHasPrivileges(this IContext context, DbObjectType objectType, ObjectName objectName, Privilege privilege) {
			var user = context.User();
			if (user == null)
				return false;

			// if no security resolver was registered this means no security
			// checks are required
			var resolver = context.GetService<IAccessController>();
			if (resolver == null)
				return false;

			if (!await resolver.HasObjectPrivilegesAsync(user.Name, objectType, objectName, privilege)) {
				var securityManager = context.GetService<ISecurityManager>();
				if (securityManager == null)
					return false;

				var roles = await securityManager.GetUserRolesAsync(user.Name);
				foreach (var role in roles) {
					if (await resolver.HasObjectPrivilegesAsync(role.Name, objectType, objectName, privilege))
						return true;
				}

				return false;
			}

			return true;
		}

		public static Task<bool> UserCanCreateInSchema(this IContext context, string schemaName) {
			return context.UserHasPrivileges(DbObjectType.Schema, new ObjectName(schemaName), SqlPrivileges.Create);
		}

		public static Task<bool> UserCanSelectFrom(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(DbObjectType.Table, tableName, SqlPrivileges.Select);

		public static Task<bool> UserCanUpdate(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(DbObjectType.Table, tableName, SqlPrivileges.Update);

		public static Task<bool> UserCanDelete(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(DbObjectType.Table, tableName, SqlPrivileges.Delete);

		public static Task<bool> UserCanInsert(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(DbObjectType.Table, tableName, SqlPrivileges.Insert);

		public static Task<bool> UserCanDrop(this IContext context, DbObjectType objectType, ObjectName objectName)
			=> context.UserHasPrivileges(objectType, objectName, SqlPrivileges.Drop);

		public static Task<bool> UserCanExecute(this IContext context, ObjectName methodName)
			=> context.UserHasPrivileges(DbObjectType.Method, methodName, SqlPrivileges.Execute);

		public static Task<bool> UserCanAlter(this IContext context, DbObjectType objectType, ObjectName objectName)
			=> context.UserHasPrivileges(objectType, objectName, SqlPrivileges.Alter);

		public static Task<bool> UserCanReference(this IContext context, ObjectName tableName)
			=> context.UserHasPrivileges(DbObjectType.Table, tableName, SqlPrivileges.References);

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
				var securityManager = context.GetService<ISecurityManager>();
				if (securityManager == null)
					return false;

				var roles = await securityManager.GetUserRolesAsync(user.Name);
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
	}
}