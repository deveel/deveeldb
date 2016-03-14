using System;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data {
	public sealed class SystemAccess : IDisposable {
		private IUserManager userManager;
		private IPrivilegeManager privilegeManager;

		public SystemAccess(ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
		}

		public ISession Session { get; private set; }

		public IUserManager UserManager {
			get {
				if (userManager == null)
					userManager = Session.Context.ResolveService<IUserManager>();

				return userManager;
			}
		}

		public IPrivilegeManager PrivilegeManager {
			get {
				if (privilegeManager == null)
					privilegeManager = Session.Context.ResolveService<IPrivilegeManager>();

				return privilegeManager;
			}
		}

		#region Security

		#region Group Management

		public void CreateUserGroup(string groupName) {
			if (!UserCanManageGroups(Session.User.Name))
				throw new InvalidOperationException(String.Format("User '{0}' has not enough privileges to create a group.", Session.User.Name));

			UserManager.CreateUserGroup(groupName);
		}

		#endregion

		#region Privilege Query

		private static bool IsSystemUser(string userName) {
			return String.Equals(userName, User.SystemName, StringComparison.OrdinalIgnoreCase);
		}

		private static bool IsPublicUser(string userName) {
			return String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase);
		}

		public string[] GetGroupsUserBelongsTo(string username) {
			return UserManager.GetUserGroups(username);
		}

		public bool UserBelongsToGroup(string group) {
			return UserBelongsToGroup(Session.User.Name, group);
		}

		public bool UserBelongsToGroup(string username, string groupName) {
			return UserManager.IsUserInGroup(username, groupName);
		}

		public bool UserCanManageGroups(string userName) {
			return IsSystemUser(userName) ||
			       UserHasSecureAccess(userName);
		}

		public bool UserCanManageGroups() {
			if (Session.User.IsSystem)
				return true;

			return UserCanManageGroups(Session.User.Name);
		}

		public bool UserHasSecureAccess() {
			if (Session.User.IsSystem)
				return true;

			return UserHasSecureAccess(Session.User.Name);
		}

		public bool UserHasSecureAccess(string userName) {
			return IsSystemUser(userName) ||
			       UserBelongsToSecureGroup(userName);
		}

		public bool UserBelongsToSecureGroup(string userName) {
			return UserBelongsToGroup(userName, SystemGroups.SecureGroup);
		}

		public bool UserBelongsToSecureGroup() {
			return UserBelongsToSecureGroup(Session.User.Name);
		}

		public bool UserHasGrantOption(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (Session.User.IsSystem)
				return true;
			if (Session.User.IsPublic)
				return false;

			return UserHasGrantOption(Session.User.Name, objectType, objectName, privileges);
		}

		public bool UserHasGrantOption(string userName, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSystemUser(userName) ||
				UserBelongsToSecureGroup(userName))
				return true;

			var grant = PrivilegeManager.GetUserPrivileges(userName, objectType, objectName, true);
			return (grant & privileges) != 0;
		}

		public bool UserHasPrivilege(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (Session.User.IsSystem)
				return true;

			return UserHasPrivilege(Session.User.Name, objectType, objectName, privileges);
		}

		public bool UserHasPrivilege(string userName, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (IsSystemUser(userName) ||
				UserBelongsToSecureGroup(userName))
				return true;

			var grant = PrivilegeManager.GetUserPrivileges(userName, objectType, objectName, false);
			return (grant & privileges) != 0;
		}

		public bool UserCanCreateUsers() {
			if (Session.User.IsSystem)
				return true;
			if (Session.User.IsPublic)
				return false;

			return UserCanCreateUsers(Session.User.Name);
		}

		public bool UserCanCreateUsers(string userName) {
			return UserHasSecureAccess(userName) ||
				UserBelongsToGroup(userName, SystemGroups.UserManagerGroup);
		}

		public bool UserCanDropUser(string userToDrop) {
			if (Session.User.IsSystem)
				return true;
			if (Session.User.IsPublic)
				return false;

			return UserCanDropUser(Session.User.Name, userToDrop);
		}

		public bool UserCanDropUser(string userName, string userToDrop) {
			return UserHasSecureAccess(userName) ||
			       UserBelongsToGroup(userName, SystemGroups.UserManagerGroup) ||
			       Session.User.Name.Equals(userToDrop, StringComparison.OrdinalIgnoreCase);
		}

		public bool UserCanAlterUser(string userToAlter) {
			if (Session.User.IsSystem)
				return true;
			if (Session.User.IsPublic)
				return false;

			return UserCanAlterUser(Session.User.Name, userToAlter);
		}

		public bool UserCanAlterUser(string userName, string userToAlter) {
			if (IsSystemUser(userName) || 
				String.Equals(userName, userToAlter, StringComparison.OrdinalIgnoreCase))
				return true;

			if (IsPublicUser(userName))
				return false;

			return UserHasSecureAccess(userName);
		}

		public bool UserCanManageUsers() {
			if (Session.User.IsSystem)
				return true;
			if (Session.User.IsPublic)
				return false;

			return UserCanManageUsers(Session.User.Name);
		}

		public bool UserCanManageUsers(string userName) {
			return UserHasSecureAccess(userName) || 
				UserBelongsToGroup(userName, SystemGroups.UserManagerGroup);
		}

		public bool UserCanAccessUsers(string userName) {
			return UserHasSecureAccess(userName) ||
			       UserBelongsToGroup(userName, SystemGroups.UserManagerGroup);
		}

		public bool UserHasTablePrivilege(ObjectName tableName, Privileges privileges) {
			return UserHasTablePrivilege(Session.User.Name, tableName, privileges);
		}

		public bool UserHasTablePrivilege(string userName, ObjectName tableName, Privileges privileges) {
			return UserHasPrivilege(userName, DbObjectType.Table, tableName, privileges);
		}

		public bool UserHasSchemaPrivilege(string schemaName, Privileges privileges) {
			return UserHasSchemaPrivilege(Session.User.Name, schemaName, privileges);
		}

		public bool UserHasSchemaPrivilege(string userName, string schemaName, Privileges privileges) {
			if (UserHasPrivilege(userName, DbObjectType.Schema, new ObjectName(schemaName), privileges))
				return true;

			return UserHasSecureAccess(userName);
		}

		public bool UserCanCreateSchema(string userName) {
			return UserHasSecureAccess(userName);
		}

		public bool UserCanCreateInSchema(string schemaName) {
			return UserCanCreateInSchema(Session.User.Name, schemaName);
		}

		public bool UserCanCreateInSchema(string userName, string schemaName) {
			return UserHasSchemaPrivilege(userName, schemaName, Privileges.Create);
		}

		public bool UserCanCreateTable(ObjectName tableName) {
			if (Session.User.IsSystem)
				return true;

			return UserCanCreateTable(Session.User.Name, tableName);
		}

		public bool UserCanCreateTable(string userName, ObjectName tableName) {
			var schema = tableName.Parent;
			if (schema == null)
				return UserHasSecureAccess(userName);

			return UserCanCreateInSchema(userName, schema.FullName);
		}

		public bool UserCanAlterInSchema(string userName, string schemaName) {
			if (UserHasSchemaPrivilege(userName, schemaName, Privileges.Alter))
				return true;

			return UserHasSecureAccess(userName);
		}

		public bool UserCanDropSchema(string userName, string schemaName) {
			if (UserCanDropObject(userName, DbObjectType.Schema, new ObjectName(schemaName)))
				return true;

			return UserHasSecureAccess(userName);
		}

		public bool UserCanAlterTable(string userName, ObjectName tableName) {
			var schema = tableName.Parent;
			if (schema == null)
				return false;

			return UserCanAlterInSchema(userName, schema.FullName);
		}

		public bool UserCanSelectFromTable(string userName, ObjectName tableName) {
			return UserCanSelectFromTable(userName, tableName, new string[0]);
		}

		public bool UserCanReferenceTable(string userName, ObjectName tableName) {
			return UserHasTablePrivilege(userName, tableName, Privileges.References);
		}

		public bool UserCanSelectFromPlan(string userName, IQueryPlanNode queryPlan) {
			var selectedTables = queryPlan.DiscoverTableNames();
			return selectedTables.All(tableName => UserCanSelectFromTable(userName, tableName));
		}

		public bool UserCanSelectFromTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Select);
		}

		public bool UserCanUpdateTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Update);
		}

		public bool UserCanInsertIntoTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Insert);
		}

		public bool UserCanExecute(RoutineType routineType, Invoke invoke, IQuery query) {
			return UserCanExecute(Session.User.Name, routineType, invoke, query);
		}

		public bool UserCanExecute(string userName, RoutineType routineType, Invoke invoke, IQuery query) {
			if (routineType == RoutineType.Function &&
				IsSystemFunction(invoke, query)) {
				return true;
			}

			if (UserHasSecureAccess(userName))
				return true;

			return UserHasPrivilege(userName, DbObjectType.Routine, invoke.RoutineName, Privileges.Execute);
		}

		public bool UserCanExecuteFunction(Invoke invoke, IQuery query) {
			return UserCanExecuteFunction(Session.User.Name, invoke, query);
		}

		public bool UserCanExecuteFunction(string userName, Invoke invoke, IQuery query) {
			return UserCanExecute(userName, RoutineType.Function, invoke, query);
		}

		public bool UserCanExecuteProcedure(Invoke invoke, IQuery query) {
			return UserCanExecuteProcedure(Session.User.Name, invoke, query);
		}

		public bool UserCanExecuteProcedure(string userName, Invoke invoke, IQuery query) {
			return UserCanExecute(userName, RoutineType.Procedure, invoke, query);
		}

		public bool UserCanCreateObject(string userName, DbObjectType objectType, ObjectName objectName) {
			return UserHasPrivilege(userName, objectType, objectName, Privileges.Create);
		}

		public bool UserCanDropObject(DbObjectType objectType, ObjectName objectName) {
			return UserCanDropObject(Session.User.Name, objectType, objectName);
		}

		public bool UserCanDropObject(string userName, DbObjectType objectType, ObjectName objectName) {
			return UserHasPrivilege(userName, objectType, objectName, Privileges.Drop);
		}

		public bool UserCanAlterObject(DbObjectType objectType, ObjectName objectName) {
			return UserCanAlterObject(Session.User.Name, objectType, objectName);
		}

		public bool UserCanAlterObject(string userName, DbObjectType objectType, ObjectName objectName) {
			return UserHasPrivilege(userName, objectType, objectName, Privileges.Alter);
		}

		public bool UserCanAccessObject(DbObjectType objectType, ObjectName objectName) {
			return UserCanAccessObject(Session.User.Name, objectType, objectName);
		}

		public bool UserCanAccessObject(string userName, DbObjectType objectType, ObjectName objectName) {
			return UserHasPrivilege(userName, objectType, objectName, Privileges.Select);
		}

		public bool UserCanDeleteFromTable(ObjectName tableName) {
			return UserCanDeleteFromTable(Session.User.Name, tableName);
		}

		public bool UserCanDeleteFromTable(string userName, ObjectName tableName) {
			return UserHasTablePrivilege(userName, tableName, Privileges.Delete);
		}

		public bool UserCanAddToGroup(string groupName) {
			return UserCanAddToGroup(Session.User.Name, groupName);
		}

		public bool UserCanAddToGroup(string userName, string groupName) {
			if (IsSystemUser(userName) ||
			    UserBelongsToSecureGroup(userName) ||
			    UserBelongsToGroup(userName, SystemGroups.UserManagerGroup))
				return true;

			return UserManager.IsUserGroupAdmin(userName, groupName);
		}


		#endregion

		#endregion

		#region Routine Management

		public bool IsSystemFunction(Invoke invoke, IQuery query) {
			var info = ResolveFunctionInfo(invoke, query);
			if (info == null)
				return false;

			return info.FunctionType != FunctionType.External &&
				   info.FunctionType != FunctionType.UserDefined;
		}

		public bool IsAggregateFunction(Invoke invoke, IQuery query) {
			var function = ResolveFunction(invoke, query);
			return function != null && function.FunctionType == FunctionType.Aggregate;
		}

		public IRoutine ResolveRoutine(Invoke invoke, IQuery query) {
			var routine = ResolveSystemRoutine(invoke, query);
			if (routine == null)
				routine = ResolveUserRoutine(invoke, query);

			return routine;
		}

		public IRoutine ResolveSystemRoutine(Invoke invoke, IQuery query) {
			// return query.SystemContext().ResolveRoutine(invoke, query);

			var resolvers = Session.Context.ResolveAllServices<IRoutineResolver>();
			foreach (var resolver in resolvers) {
				var routine = resolver.ResolveRoutine(invoke, query);
				if (routine != null)
					return routine;
			}

			return null;
		}

		public IRoutine ResolveUserRoutine(Invoke invoke, IQuery query) {
			var routine = Session.ResolveRoutine(invoke);
			if (routine != null &&
				!UserCanExecute(routine.Type, invoke, query))
				throw new InvalidOperationException();

			return routine;
		}

		public IFunction ResolveFunction(Invoke invoke, IQuery query) {
			return ResolveRoutine(invoke, query) as IFunction;
		}

		public IFunction ResolveFunction(IQuery query, ObjectName functionName, params SqlExpression[] args) {
			var invoke = new Invoke(functionName, args);
			return ResolveFunction(invoke, query);
		}

		public FunctionInfo ResolveFunctionInfo(Invoke invoke, IQuery query) {
			return ResolveRoutineInfo(invoke, query) as FunctionInfo;
		}

		public RoutineInfo ResolveRoutineInfo(Invoke invoke, IQuery query) {
			var routine = ResolveRoutine(invoke, query);
			if (routine == null)
				return null;

			return routine.RoutineInfo;
		}

		#endregion

		public void Dispose() {
			userManager = null;
			privilegeManager = null;
			Session = null;
		}
	}
}
