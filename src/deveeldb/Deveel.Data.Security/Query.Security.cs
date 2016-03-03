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

using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Security {
	public static class Query {
		private static IUserManager UserManager(this IQuery query) {
			return query.Context.ResolveService<IUserManager>();
		}

		private static IPrivilegeManager PrivilegeManager(this IQuery query) {
			return query.Context.ResolveService<IPrivilegeManager>();
		}

		public static void CreateUserGroup(this IQuery query, string groupName) {
			if (!query.UserCanManageGroups())
				throw new InvalidOperationException(String.Format("User '{0}' has not enough privileges to create a group.", query.UserName()));

			query.Direct().UserManager().CreateUserGroup(groupName);
		}

		#region User Management

		public static User GetUser(this IQuery query, string userName) {
			if (query.UserName().Equals(userName, StringComparison.OrdinalIgnoreCase))
				return new User(userName);

			if (!query.UserCanAccessUsers())
				throw new MissingPrivilegesException(query.UserName(), new ObjectName(userName), Privileges.Select,
					String.Format("The user '{0}' has not enough rights to access other users information.", query.UserName()));

			if (!query.Direct().UserManager().UserExists(userName))
				return null;

			return new User(userName);
		}

		public static void SetUserStatus(this IQuery queryContext, string username, UserStatus status) {
			if (!queryContext.UserCanManageUsers())
				throw new MissingPrivilegesException(queryContext.UserName(), new ObjectName(username), Privileges.Alter,
					String.Format("User '{0}' cannot change the status of user '{1}'", queryContext.UserName(), username));

			queryContext.Direct().UserManager().SetUserStatus(username, status);
		}

		public static UserStatus GetUserStatus(this IQuery queryContext, string userName) {
			if (!queryContext.UserName().Equals(userName) &&
				!queryContext.UserCanAccessUsers())
				throw new MissingPrivilegesException(queryContext.UserName(), new ObjectName(userName), Privileges.Select,
					String.Format("The user '{0}' has not enough rights to access other users information.", queryContext.UserName()));

			return queryContext.Direct().UserManager().GetUserStatus(userName);
		}

		public static void SetUserGroups(this IQuery query, string userName, string[] groups) {
			if (!query.UserCanManageUsers())
				throw new MissingPrivilegesException(query.UserName(), new ObjectName(userName), Privileges.Alter,
					String.Format("The user '{0}' has not enough rights to modify other users information.", query.UserName()));

			// TODO: Check if the user exists?

			var userGroups = query.Direct().UserManager().GetUserGroups(userName);
			foreach (var userGroup in userGroups) {
				query.Direct().UserManager().RemoveUserFromGroup(userName, userGroup);
			}

			foreach (var userGroup in groups) {
				query.Direct().UserManager().AddUserToGroup(userName, userGroup, false);
			}
		}

		public static bool UserExists(this IQuery query, string userName) {
			return query.Direct().UserManager().UserExists(userName);
		}

		public static void CreatePublicUser(this IQuery query) {
			if (!query.User().IsSystem)
				throw new InvalidOperationException("The @PUBLIC user can be created only by the SYSTEM");

			var userName = User.PublicName;
			var userId = UserIdentification.PlainText;
			var userInfo = new UserInfo(userName, userId);

			query.Direct().UserManager().CreateUser(userInfo, "####");
		}

		public static User CreateUser(this IQuery query, string userName, string password) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			if (!query.UserCanCreateUsers())
				throw new MissingPrivilegesException(userName, new ObjectName(userName), Privileges.Create,
					String.Format("User '{0}' cannot create users.", query.UserName()));

			if (String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(
					String.Format("User name '{0}' is reserved and cannot be registered.", User.PublicName), "userName");

			if (userName.Length <= 1)
				throw new ArgumentException("User name must be at least one character.");
			if (password.Length <= 1)
				throw new ArgumentException("The password must be at least one character.");

			var c = userName[0];
			if (c == '#' || c == '@' || c == '$' || c == '&')
				throw new ArgumentException(
					String.Format("User name '{0}' is invalid: cannot start with '{1}' character.", userName, c), "userName");

			var userId = UserIdentification.PlainText;
			var userInfo = new UserInfo(userName, userId);

			query.Direct().UserManager().CreateUser(userInfo, password);

			return new User(userName);
		}

		public static void AlterUserPassword(this IQuery queryContext, string username, string password) {
			if (!queryContext.UserCanAlterUser(username))
				throw new MissingPrivilegesException(queryContext.UserName(), new ObjectName(username), Privileges.Alter);

			var userId = UserIdentification.PlainText;
			var userInfo = new UserInfo(username, userId);

			queryContext.Direct().UserManager().AlterUser(userInfo, password);
		}

		public static bool DeleteUser(this IQuery query, string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			if (!query.UserCanDropUser(userName))
				throw new MissingPrivilegesException(query.UserName(), new ObjectName(userName), Privileges.Drop);

			return query.Direct().UserManager().DropUser(userName);
		}

		/// <summary>
		/// Authenticates the specified user using the provided credentials.
		/// </summary>
		/// <param name="queryContext">The query query.</param>
		/// <param name="username">The name of the user to authenticate.</param>
		/// <param name="password">The password used to authenticate the user.</param>
		/// <returns></returns>
		/// <exception cref="System.ArgumentNullException">
		/// If either <paramref name="username"/> or <paramref name="password"/> are
		/// <c>null</c> or empty.
		/// </exception>
		/// <exception cref="SecurityException">
		/// If the authentication was not successful for the credentials provided.
		/// </exception>
		/// <exception cref="System.NotImplementedException">The external authentication mechanism is not implemented yet</exception>
		public static User Authenticate(this IQuery queryContext, string username, string password) {
			try {
				if (String.IsNullOrEmpty(username))
					throw new ArgumentNullException("username");
				if (String.IsNullOrEmpty(password))
					throw new ArgumentNullException("password");

				var userInfo = queryContext.Direct().UserManager().GetUser(username);

				if (userInfo == null)
					return null;

				var userId = userInfo.Identification;

				if (userId.Method != "plain")
					throw new NotImplementedException();

				if (!queryContext.Direct().UserManager().CheckIdentifier(username, password))
					return null;

				// Successfully authenticated...
				return new User(username);
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new SecurityException("Could not authenticate user.", ex);
			}
		}

		#endregion

		#region User Grants Management

		public static void AddUserToGroup(this IQuery queryContext, string username, string group, bool asAdmin = false) {
			if (String.IsNullOrEmpty(@group))
				throw new ArgumentNullException("group");
			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");

			if (!queryContext.UserCanAddToGroup(group))
				throw new SecurityException();

			queryContext.Direct().UserManager().AddUserToGroup(username, group, asAdmin);
		}

		public static void GrantToUserOn(this IQuery query, ObjectName objectName, string grantee, Privileges privileges, bool withOption = false) {
			var obj = query.FindObject(objectName);
			if (obj == null)
				throw new ObjectNotFoundException(objectName);

			query.GrantToUserOn(obj.ObjectType, obj.FullName, grantee, privileges, withOption);
		}

		public static void GrantToUserOn(this IQuery query, DbObjectType objectType, ObjectName objectName, string grantee, Privileges privileges, bool withOption = false) {
			if (String.Equals(grantee, User.SystemName))       // The @SYSTEM user does not need any other
				return;

			if (!query.ObjectExists(objectType, objectName))
				throw new ObjectNotFoundException(objectName);

			if (!query.UserHasGrantOption(objectType, objectName, privileges))
				throw new MissingPrivilegesException(query.UserName(), objectName, privileges);

			var granter = query.UserName();
			var grant = new Grant(privileges, objectName, objectType, granter, withOption);
			query.Direct().PrivilegeManager().GrantToUser(grantee, grant);
		}

		public static void GrantToUserOnSchema(this IQuery query, string schemaName, string grantee, Privileges privileges, bool withOption = false) {
			query.GrantToUserOn(DbObjectType.Schema, new ObjectName(schemaName), grantee, privileges, withOption);
		}

		public static void GrantToGroupOn(this IQuery query, DbObjectType objectType, ObjectName objectName, string groupName, Privileges privileges, bool withOption = false) {
			if (SystemGroups.IsSystemGroup(groupName))
				throw new InvalidOperationException("Cannot grant to a system group.");

			if (!query.UserCanManageGroups())
				throw new MissingPrivilegesException(query.UserName(), new ObjectName(groupName));

			if (!query.ObjectExists(objectType, objectName))
				throw new ObjectNotFoundException(objectName);

			var granter = query.UserName();
			var grant = new Grant(privileges, objectName, objectType, granter, withOption);
			query.Direct().PrivilegeManager().GrantToGroup(groupName, grant);
		}

		public static void GrantTo(this IQuery query, string groupOrUserName, DbObjectType objectType, ObjectName objectName, Privileges privileges, bool withOption = false) {
			if (query.Direct().UserManager().UserGroupExists(groupOrUserName)) {
				if (withOption)
					throw new SecurityException("User groups cannot be granted with grant option.");

				query.GrantToGroupOn(objectType, objectName, groupOrUserName, privileges);
			} else if (query.Direct().UserManager().UserExists(groupOrUserName)) {
				query.GrantToUserOn(objectType, objectName, groupOrUserName, privileges, withOption);
			} else {
				throw new SecurityException(String.Format("User or group '{0}' was not found.", groupOrUserName));
			}
		}

		public static void RevokeAllGrantsOnTable(this IQuery query, ObjectName objectName) {
			RevokeAllGrantsOn(query, DbObjectType.Table, objectName);
		}

		public static void RevokeAllGrantsOnView(this IQuery query, ObjectName objectName) {
			query.RevokeAllGrantsOn(DbObjectType.View, objectName);
		}

		public static void RevokeAllGrantsOn(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			var grantTable = query.GetMutableTable(SystemSchema.UserGrantsTableName);

			var objectTypeColumn = grantTable.GetResolvedColumnName(1);
			var objectNameColumn = grantTable.GetResolvedColumnName(2);
			// All that match the given object
			var t1 = grantTable.SimpleSelect(query, objectTypeColumn, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.Integer((int)objectType)));
			// All that match the given parameter
			t1 = t1.SimpleSelect(query, objectNameColumn, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.String(objectName.FullName)));

			// Remove these rows from the table
			grantTable.Delete(t1);
		}

		public static void GrantToUserOnTable(this IQuery query, ObjectName tableName, string grantee, Privileges privileges) {
			query.GrantToUserOn(DbObjectType.Table, tableName, grantee, privileges);
		}

		#endregion

		#region User Grants Query

		public static string[] GetGroupsUserBelongsTo(this IQuery queryContext, string username) {
			return queryContext.Direct().UserManager().GetUserGroups(username);
		}

		public static bool UserBelongsToGroup(this IQuery queryContext, string group) {
			return UserBelongsToGroup(queryContext, queryContext.UserName(), group);
		}

		public static bool UserBelongsToGroup(this IQuery query, string username, string groupName) {
			return query.Direct().UserManager().IsUserInGroup(username, groupName);
		}

		public static bool UserCanManageGroups(this IQuery query) {
			return query.User().IsSystem || query.UserHasSecureAccess();
		}

		public static bool UserHasSecureAccess(this IQuery query) {
			if (query.User().IsSystem)
				return true;

			return query.UserBelongsToSecureGroup();
		}

		public static bool UserBelongsToSecureGroup(this IQuery query) {
			return query.UserBelongsToGroup(SystemGroups.SecureGroup);
		}

		public static bool UserHasGrantOption(this IQuery query, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			var user = query.User();
			if (user.IsSystem)
				return true;

			if (query.UserBelongsToSecureGroup())
				return true;

			var grant = query.Direct().PrivilegeManager().GetUserPrivileges(user.Name, objectType, objectName, true);
			return (grant & privileges) != 0;
		}

		public static bool UserHasPrivilege(this IQuery query, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			var user = query.User();
			if (user.IsSystem)
				return true;

			if (query.UserBelongsToSecureGroup())
				return true;

			var userName = user.Name;
			var grant = query.Direct().PrivilegeManager().GetUserPrivileges(userName, objectType, objectName, false);
			return (grant & privileges) != 0;
		}

		public static bool UserCanCreateUsers(this IQuery query) {
			return query.UserHasSecureAccess() ||
				query.UserBelongsToGroup(SystemGroups.UserManagerGroup);
		}

		public static bool UserCanDropUser(this IQuery query, string userToDrop) {
			return query.UserHasSecureAccess() ||
				   query.UserBelongsToGroup(SystemGroups.UserManagerGroup) ||
				   query.UserName().Equals(userToDrop, StringComparison.OrdinalIgnoreCase);
		}

		public static bool UserCanAlterUser(this IQuery query, string userName) {
			if (query.UserName().Equals(userName))
				return true;

			if (userName.Equals(User.PublicName, StringComparison.OrdinalIgnoreCase))
				return false;

			return query.UserHasSecureAccess();
		}

		public static bool UserCanManageUsers(this IQuery query) {
			return query.UserHasSecureAccess() || query.UserBelongsToGroup(SystemGroups.UserManagerGroup);
		}

		public static bool UserCanAccessUsers(this IQuery query) {
			return query.UserHasSecureAccess() || query.UserBelongsToGroup(SystemGroups.UserManagerGroup);
		}

		public static bool UserHasTablePrivilege(this IQuery query, ObjectName tableName, Privileges privileges) {
			return query.UserHasPrivilege(DbObjectType.Table, tableName, privileges);
		}

		public static bool UserHasSchemaPrivilege(this IQuery query, string schemaName, Privileges privileges) {
			if (query.UserHasPrivilege(DbObjectType.Schema, new ObjectName(schemaName), privileges))
				return true;

			return query.UserHasSecureAccess();
		}

		public static bool UserCanCreateSchema(this IQuery query) {
			return query.UserHasSecureAccess();
		}

		public static bool UserCanCreateInSchema(this IQuery query, string schemaName) {
			return query.UserHasSchemaPrivilege(schemaName, Privileges.Create);
		}

		public static bool UserCanCreateTable(this IQuery query, ObjectName tableName) {
			var schema = tableName.Parent;
			if (schema == null)
				return query.UserHasSecureAccess();

			return query.UserCanCreateInSchema(schema.FullName);
		}

		public static bool UserCanAlterInSchema(this IQuery query, string schemaName) {
			if (query.UserHasSchemaPrivilege(schemaName, Privileges.Alter))
				return true;

			return query.UserHasSecureAccess();
		}

		public static bool UserCanDropSchema(this IQuery query, string schemaName) {
			if (query.UserCanDropObject(DbObjectType.Schema, new ObjectName(schemaName)))
				return true;

			return query.UserHasSecureAccess();
		}

		public static bool UserCanAlterTable(this IQuery query, ObjectName tableName) {
			var schema = tableName.Parent;
			if (schema == null)
				return false;

			return query.UserCanAlterInSchema(schema.FullName);
		}

		public static bool UserCanSelectFromTable(this IQuery query, ObjectName tableName) {
			return UserCanSelectFromTable(query, tableName, new string[0]);
		}

		public static bool UserCanReferenceTable(this IQuery query, ObjectName tableName) {
			return query.UserHasTablePrivilege(tableName, Privileges.References);
		}

		public static bool UserCanSelectFromPlan(this IQuery query, IQueryPlanNode queryPlan) {
			var selectedTables = queryPlan.DiscoverTableNames();
			return selectedTables.All(query.UserCanSelectFromTable);
		}

		public static bool UserCanSelectFromTable(this IQuery query, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			return query.UserHasTablePrivilege(tableName, Privileges.Select);
		}

		public static bool UserCanUpdateTable(this IQuery query, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			return query.UserHasTablePrivilege(tableName, Privileges.Update);
		}

		public static bool UserCanInsertIntoTable(this IQuery query, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			return query.UserHasTablePrivilege(tableName, Privileges.Insert);
		}

		public static bool UserCanExecute(this IQuery query, RoutineType routineType, Invoke invoke) {
			if (routineType == RoutineType.Function &&
				query.IsSystemFunction(invoke)) {
				return true;
			}

			if (query.UserHasSecureAccess())
				return true;

			return query.UserHasPrivilege(DbObjectType.Routine, invoke.RoutineName, Privileges.Execute);
		}

		public static bool UserCanExecuteFunction(this IQuery query, Invoke invoke) {
			return query.UserCanExecute(RoutineType.Function, invoke);
		}

		public static bool UserCanExecuteProcedure(this IQuery query, Invoke invoke) {
			return query.UserCanExecute(RoutineType.Procedure, invoke);
		}

		public static bool UserCanCreateObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.UserHasPrivilege(objectType, objectName, Privileges.Create);
		}

		public static bool UserCanDropObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.UserHasPrivilege(objectType, objectName, Privileges.Drop);
		}

		public static bool UserCanAlterObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.UserHasPrivilege(objectType, objectName, Privileges.Alter);
		}

		public static bool UserCanAccessObject(this IQuery query, DbObjectType objectType, ObjectName objectName) {
			return query.UserHasPrivilege(objectType, objectName, Privileges.Select);
		}

		public static bool UserCanDeleteFromTable(this IQuery query, ObjectName tableName) {
			return query.UserHasTablePrivilege(tableName, Privileges.Delete);
		}

		public static bool UserCanAddToGroup(this IQuery query, string groupName) {
			if (query.User().IsSystem)
				return true;

			if (query.UserBelongsToSecureGroup() ||
				query.UserBelongsToGroup(SystemGroups.UserManagerGroup))
				return true;

			return query.Direct().UserManager().IsUserGroupAdmin(query.UserName(), groupName);
		}

		#endregion

	}
}
