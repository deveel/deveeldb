using System;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Views;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public abstract class SystemAccess {
		private IUserManager userManager;
		private IPrivilegeManager privilegeManager;

		protected abstract ISession Session { get; }

		private IUserManager UserManager {
			get {
				if (userManager == null)
					userManager = Session.Context.ResolveService<IUserManager>();

				return userManager;
			}
		}

		private IPrivilegeManager PrivilegeManager {
			get {
				if (privilegeManager == null)
					privilegeManager = Session.Context.ResolveService<IPrivilegeManager>();

				return privilegeManager;
			}
		}

				#region Objects

		public virtual IDbObject GetObject(DbObjectType objectType, ObjectName objectName) {
			return GetObject(objectType, objectName, AccessType.ReadWrite);
		}

		public virtual IDbObject GetObject(DbObjectType objectType, ObjectName objectName, AccessType accessType) {
			return Session.Transaction.GetObject(objectType, objectName);
		}

		public virtual void CreateObject(IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!UserCanCreateObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			Session.Transaction.CreateObject(objectInfo);
		}

		public virtual void AlterObject(IObjectInfo objectInfo) {
			if (!UserCanAlterObject(objectInfo.ObjectType, objectInfo.FullName))
				throw new MissingPrivilegesException(Session.User.Name, objectInfo.FullName, Privileges.Alter);

			Session.Transaction.AlterObject(objectInfo);
		}

		public virtual bool ObjectExists(ObjectName objectName) {
			return Session.Transaction.ObjectExists(objectName);
		}

		public virtual bool ObjectExists(DbObjectType objectType, ObjectName objectName) {
			return Session.Transaction.ObjectExists(objectType, objectName);
		}

		public virtual IDbObject FindObject(ObjectName objectName) {
			return Session.Transaction.FindObject(objectName);
		}

		public virtual bool DropObject(DbObjectType objectType, ObjectName objectName) {
			if (!Session.Access.UserCanDropObject(objectType, objectName))
				throw new MissingPrivilegesException(Session.User.Name, objectName, Privileges.Drop);

			return Session.Transaction.DropObject(objectType, objectName);
		}

		public virtual ObjectName ResolveObjectName(string name) {
			return Session.Transaction.ResolveObjectName(name);
		}

		public virtual ObjectName ResolveObjectName(DbObjectType objectType, ObjectName objectName) {
			return Session.Transaction.ResolveObjectName(objectType, objectName);
		}

		public virtual ObjectName ResolveObjectName(ObjectName objectName) {
			return Session.Transaction.ResolveObjectName(objectName);
		}

		#endregion

		#region Schemata

		public void CreateSchema(string name, string type) {
			if (!UserCanCreateSchema())
				throw new InvalidOperationException();      // TODO: throw a specialized exception

			CreateObject(new SchemaInfo(name, type));
		}

		public void DropSchema(string schemaName) {
			if (!UserCanDropSchema(schemaName))
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(schemaName), Privileges.Drop);

			DropObject(DbObjectType.Schema, new ObjectName(schemaName));
		}

		public bool SchemaExists(string name) {
			return ObjectExists(DbObjectType.Schema, new ObjectName(name));
		}

		public ObjectName ResolveSchemaName(string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			return ResolveObjectName(DbObjectType.Schema, new ObjectName(name));
		}

		#endregion

		#region Tables


		public ObjectName ResolveTableName(ObjectName tableName) {
			return Session.Transaction.ResolveObjectName(DbObjectType.Table, tableName);
		}

		public TableInfo GetTableInfo(ObjectName tableName) {
			return Session.Transaction.GetTableInfo(tableName);
		}

		public string GetTableType(ObjectName tableName) {
			return Session.Transaction.GetTableType(tableName);
		}

		public void CreateTable(TableInfo tableInfo) {
			CreateTable(tableInfo, false);
		}

		public virtual void CreateTable(TableInfo tableInfo, bool temporary) {
			Session.Transaction.CreateTable(tableInfo, temporary);
		}

		public virtual ITable GetTable(ObjectName tableName) {
			return GetObject(DbObjectType.Table, tableName, AccessType.ReadWrite) as ITable;
		}

		#region Constraints

		public void AddPrimaryKey(ObjectName tableName, string column) {
			AddPrimaryKey(tableName, column, null);
		}

		public void AddPrimaryKey(ObjectName tableName, string column, string constraintName) {
			AddPrimaryKey(tableName, new []{column}, constraintName);
		}

		public void AddPrimaryKey(ObjectName tableName, string[] columns, string constraintName) {
			AddPrimaryKey(tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public void AddPrimaryKey(ObjectName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			if (!UserCanAlterTable(tableName))
				throw new MissingPrivilegesException(Session.User.Name, tableName, Privileges.Alter);

			Session.Transaction.AddPrimaryKey(tableName, columns, deferred, constraintName);
		}

		public void AddForeignKey(ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, String constraintName) {
			AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public void AddForeignKey(ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred, string constraintName) {
			Session.Transaction.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public void AddUniqueKey(ObjectName tableName, string[] columns, string constraintName) {
			AddUniqueKey(tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public void AddUniqueKey(ObjectName tableName, string[] columns, ConstraintDeferrability deferrability, string constraintName) {
			Session.Transaction.AddUniqueKey(tableName, columns, deferrability, constraintName);
		}

		public void AddCheck(ObjectName tableName, SqlExpression expression, string constraintName) {
			AddCheck(tableName, expression, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public void AddCheck(ObjectName tableName, SqlExpression expression, ConstraintDeferrability deferrability, string constraintName) {
			Session.Transaction.AddCheck(tableName, expression, deferrability, constraintName);
		}

		public void AddConstraint(ObjectName tableName, ConstraintInfo constraintInfo) {
			if (constraintInfo.ConstraintType == ConstraintType.PrimaryKey) {
				var columnNames = constraintInfo.ColumnNames;
				if (columnNames.Length > 1)
					throw new ArgumentException();

				AddPrimaryKey(tableName, columnNames[0], constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Unique) {
				AddUniqueKey(tableName, constraintInfo.ColumnNames, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Check) {
				AddCheck(tableName, constraintInfo.CheckExpression, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.ForeignKey) {
				AddForeignKey(tableName, constraintInfo.ColumnNames, constraintInfo.ForeignTable,
					constraintInfo.ForeignColumnNames, constraintInfo.OnDelete, constraintInfo.OnUpdate, constraintInfo.ConstraintName);
			}
		}

		public void DropAllTableConstraints(ObjectName tableName) {
			Session.Transaction.DropAllTableConstraints(tableName);
		}

		public int DropTableConstraint(ObjectName tableName, string constraintName) {
			return Session.Transaction.DropTableConstraint(tableName, constraintName);
		}

		public bool DropTablePrimaryKey(ObjectName tableName, string constraintName) {
			return Session.Transaction.DropTablePrimaryKey(tableName, constraintName);
		}

		public ObjectName[] QueryTablesRelationallyLinkedTo(ObjectName tableName) {
			return Session.Transaction.QueryTablesRelationallyLinkedTo(tableName);
		}

		public ConstraintInfo[] QueryTableCheckExpressions(ObjectName tableName) {
			return Session.Transaction.QueryTableCheckExpressions(tableName);
		}

		public ConstraintInfo QueryTablePrimaryKey(ObjectName tableName) {
			return Session.Transaction.QueryTablePrimaryKey(tableName);
		}

		public ConstraintInfo[] QueryTableUniqueKeys(ObjectName tableName) {
			return Session.Transaction.QueryTableUniqueKeys(tableName);
		}

		public ConstraintInfo[] QueryTableImportedForeignKeys(ObjectName refTableName) {
			return Session.Transaction.QueryTableImportedForeignKeys(refTableName);
		}

		public ConstraintInfo[] QueryTableForeignKeys(ObjectName tableName) {
			return Session.Transaction.QueryTableForeignKeys(tableName);
		}

		public void CheckConstraintViolations(ObjectName tableName) {
			Session.Transaction.CheckAllConstraintViolations(tableName);
		}

		public ObjectName ResolveTableName(string name) {
			var schema = Session.CurrentSchema;
			if (String.IsNullOrEmpty(schema))
				throw new InvalidOperationException("Default schema not specified in the query.");

			var objSchemaName = ResolveSchemaName(schema);
			if (objSchemaName == null)
				throw new InvalidOperationException(
					String.Format("The default schema of the session '{0}' is not defined in the database.", schema));

			var objName = ObjectName.Parse(name);
			if (objName.Parent == null)
				objName = new ObjectName(objSchemaName, objName.Name);

			return ResolveTableName(objName);
		}

		public bool TableExists(ObjectName tableName) {
			return ObjectExists(DbObjectType.Table, tableName);
		}

		public void CreateSystemTable(TableInfo tableInfo) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!UserCanCreateTable(tableName))
				throw new MissingPrivilegesException(Session.User.Name, tableName, Privileges.Create);

			CreateTable(tableInfo, false);
		}

		#endregion

		#region Query

		public ITableQueryInfo GetTableQueryInfo(ObjectName tableName, ObjectName alias) {
			var tableInfo = GetTableInfo(tableName);
			if (alias != null) {
				tableInfo = tableInfo.Alias(alias);
			}

			return new TableQueryInfo(Session, tableInfo, tableName, alias);
		}

		public IQueryPlanNode CreateQueryPlan(ObjectName tableName, ObjectName aliasedName) {
			string tableType = GetTableType(tableName);
			if (tableType.Equals(TableTypes.View))
				return new FetchViewNode(tableName, aliasedName);

			return new FetchTableNode(tableName, aliasedName);
		}

		#region TableQueryInfo

		class TableQueryInfo : ITableQueryInfo {
			public TableQueryInfo(ISession session, TableInfo tableInfo, ObjectName tableName, ObjectName aliasName) {
				Session = session;
				TableInfo = tableInfo;
				TableName = tableName;
				AliasName = aliasName;
			}

			public ISession Session { get; private set; }

			public TableInfo TableInfo { get; private set; }

			public ObjectName TableName { get; set; }

			public ObjectName AliasName { get; set; }

			public IQueryPlanNode QueryPlanNode {
				get { return Session.Access.CreateQueryPlan(TableName, AliasName); }
			}
		}

		#endregion

		#endregion

		#endregion

		#region Sequences

		public ISequence GetSequence(ObjectName sequenceName) {
			return GetObject(DbObjectType.Sequence, sequenceName, AccessType.Read) as ISequence;
		}

		/// <summary>
		/// Increments the sequence and returns the computed value.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence to increment and
		/// whose incremented value must be returned.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the result of
		/// the increment operation over the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public SqlNumber GetNextValue(ObjectName sequenceName) {
			var sequence = GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.NextValue();
		}

		/// <summary>
		/// Gets the current value of the sequence.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence whose current value
		/// must be obtained.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the current value
		/// of the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public SqlNumber GetCurrentValue(ObjectName sequenceName) {
			var sequence = GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.GetCurrentValue();
		}

		/// <summary>
		/// Sets the current value of the sequence, overriding the increment
		/// mechanism in place.
		/// </summary>
		/// <param name="sequenceName">The name of the sequence whose current state
		/// to be set.</param>
		/// <param name="value">The numeric value to set.</param>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public void SetCurrentValue(ObjectName sequenceName, SqlNumber value) {
			var sequence = GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			sequence.SetValue(value);
		}

		#endregion

		#region Views

		public bool ViewExists(ObjectName viewName) {
			return ObjectExists(DbObjectType.View, viewName);
		}

		public View GetView(ObjectName viewName) {
			return GetObject(DbObjectType.View, viewName, AccessType.Read) as View;
		}

		public IQueryPlanNode GetViewQueryPlan(ObjectName viewName) {
			var view = GetView(viewName);
			return view == null ? null : view.QueryPlan;
		}

		#endregion

		#region Triggers

		public void FireTriggers(IRequest context, TableEvent tableEvent) {
			var manager = Session.Transaction.GetTriggerManager();
			if (manager == null)
				return;

			manager.FireTriggers(context, tableEvent);
		}

		public void CreateTrigger(TriggerInfo triggerInfo) {
			CreateObject(triggerInfo);
		}

		public void CreateCallbackTrigger(ObjectName triggerName, TriggerEventType eventType) {
			// TODO: Create it in the session context
			CreateTrigger(new TriggerInfo(triggerName, eventType));
		}

		public bool TriggerExists(ObjectName triggerName) {
			// TODO: verify the callback triggers
			return ObjectExists(DbObjectType.Trigger, triggerName);
		}

		#endregion

		#region Security

		#region Group Management

		public void CreateUserGroup(string groupName) {
			if (!UserCanManageGroups(Session.User.Name))
				throw new InvalidOperationException(String.Format("User '{0}' has not enough privileges to create a group.", Session.User.Name));

			UserManager.CreateUserGroup(groupName);
		}

		#endregion

		#region User Management

		public  User GetUser(string userName) {
			if (Session.User.Name.Equals(userName, StringComparison.OrdinalIgnoreCase))
				return new User(userName);

			if (!UserCanAccessUsers())
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(userName), Privileges.Select,
					String.Format("The user '{0}' has not enough rights to access other users information.", Session.User.Name));

			if (!UserManager.UserExists(userName))
				return null;

			return new User(userName);
		}

		public void SetUserStatus(string username, UserStatus status) {
			if (!UserCanManageUsers())
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(username), Privileges.Alter,
					String.Format("User '{0}' cannot change the status of user '{1}'", Session.User.Name, username));

			UserManager.SetUserStatus(username, status);
		}

		public UserStatus GetUserStatus(string userName) {
			if (!Session.User.Name.Equals(userName) &&
				!UserCanAccessUsers())
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(userName), Privileges.Select,
					String.Format("The user '{0}' has not enough rights to access other users information.", Session.User.Name));

			return UserManager.GetUserStatus(userName);
		}

		public void SetUserGroups(string userName, string[] groups) {
			if (!UserCanManageUsers())
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(userName), Privileges.Alter,
					String.Format("The user '{0}' has not enough rights to modify other users information.", Session.User.Name));

			// TODO: Check if the user exists?

			var userGroups = UserManager.GetUserGroups(userName);
			foreach (var userGroup in userGroups) {
				UserManager.RemoveUserFromGroup(userName, userGroup);
			}

			foreach (var userGroup in groups) {
				UserManager.AddUserToGroup(userName, userGroup, false);
			}
		}

		public bool UserExists(string userName) {
			return UserManager.UserExists(userName);
		}

		public void CreatePublicUser() {
			if (!Session.User.IsSystem)
				throw new InvalidOperationException("The @PUBLIC user can be created only by the SYSTEM");

			var userName = User.PublicName;
			var userId = UserIdentification.PlainText;
			var userInfo = new UserInfo(userName, userId);

			UserManager.CreateUser(userInfo, "####");
		}

		public User CreateUser(string userName, string password) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			if (!UserCanCreateUsers())
				throw new MissingPrivilegesException(userName, new ObjectName(userName), Privileges.Create,
					String.Format("User '{0}' cannot create users.", Session.User.Name));

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

			UserManager.CreateUser(userInfo, password);

			return new User(userName);
		}

		public void AlterUserPassword(string username, string password) {
			if (!UserCanAlterUser(username))
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(username), Privileges.Alter);

			var userId = UserIdentification.PlainText;
			var userInfo = new UserInfo(username, userId);

			UserManager.AlterUser(userInfo, password);
		}

		public bool DeleteUser(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			if (!UserCanDropUser(userName))
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(userName), Privileges.Drop);

			return UserManager.DropUser(userName);
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
		public User Authenticate(string username, string password) {
			try {
				if (String.IsNullOrEmpty(username))
					throw new ArgumentNullException("username");
				if (String.IsNullOrEmpty(password))
					throw new ArgumentNullException("password");

				var userInfo = UserManager.GetUser(username);

				if (userInfo == null)
					return null;

				var userId = userInfo.Identification;

				if (userId.Method != "plain")
					throw new NotImplementedException();

				if (!UserManager.CheckIdentifier(username, password))
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

		public bool UserCanAccessUsers() {
			if (Session.User.IsSystem)
				return true;

			return UserCanAccessUsers(Session.User.Name);
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

		public bool UserCanCreateSchema() {
			return UserCanCreateSchema(Session.User.Name);
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

		public bool UserCanDropSchema(string schemaName) {
			return UserCanDropSchema(Session.User.Name, schemaName);
		}

		public bool UserCanDropSchema(string userName, string schemaName) {
			if (UserCanDropObject(userName, DbObjectType.Schema, new ObjectName(schemaName)))
				return true;

			return UserHasSecureAccess(userName);
		}

		public bool UserCanAlterTable(ObjectName tableName) {
			return UserCanAlterTable(Session.User.Name, tableName);
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

		public bool UserCanSelectFromPlan(IQueryPlanNode queryPlan) {
			return UserCanSelectFromPlan(Session.User.Name, queryPlan);
		}

		public bool UserCanSelectFromPlan(string userName, IQueryPlanNode queryPlan) {
			var selectedTables = queryPlan.DiscoverTableNames();
			return selectedTables.All(tableName => UserCanSelectFromTable(userName, tableName));
		}

		public bool UserCanSelectFromTable(ObjectName tableName, params string[] columnNames) {
			return UserCanSelectFromTable(Session.User.Name, tableName, columnNames);
		}

		public bool UserCanSelectFromTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Select);
		}

		public bool UserCanUpdateTable(ObjectName tableName, params string[] columnNames) {
			return UserCanUpdateTable(Session.User.Name, tableName, columnNames);
		}

		public bool UserCanUpdateTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Update);
		}

		public bool UserCanInsertIntoTable(ObjectName tableName, params string[] columnNames) {
			return UserCanInsertIntoTable(Session.User.Name, tableName, columnNames);
		}

		public bool UserCanInsertIntoTable(string userName, ObjectName tableName, params string[] columnNames) {
			// TODO: Column-level select will be implemented in the future
			if (columnNames != null && columnNames.Length > 0)
				throw new NotSupportedException();

			return UserHasTablePrivilege(userName, tableName, Privileges.Insert);
		}

		public bool UserCanExecute(RoutineType routineType, Invoke invoke, IRequest request) {
			return UserCanExecute(Session.User.Name, routineType, invoke, request);
		}

		public bool UserCanExecute(string userName, RoutineType routineType, Invoke invoke, IRequest request) {
			if (routineType == RoutineType.Function &&
				IsSystemFunction(invoke, request)) {
				return true;
			}

			if (UserHasSecureAccess(userName))
				return true;

			return UserHasPrivilege(userName, DbObjectType.Routine, invoke.RoutineName, Privileges.Execute);
		}

		public bool UserCanExecuteFunction(Invoke invoke, IRequest request) {
			return UserCanExecuteFunction(Session.User.Name, invoke, request);
		}

		public bool UserCanExecuteFunction(string userName, Invoke invoke, IRequest request) {
			return UserCanExecute(userName, RoutineType.Function, invoke, request);
		}

		public bool UserCanExecuteProcedure(Invoke invoke, IRequest request) {
			return UserCanExecuteProcedure(Session.User.Name, invoke, request);
		}

		public bool UserCanExecuteProcedure(string userName, Invoke invoke, IRequest request) {
			return UserCanExecute(userName, RoutineType.Procedure, invoke, request);
		}

		public bool UserCanCreateObject(DbObjectType objectType, ObjectName objectName) {
			if (Session.User.IsSystem)
				return true;

			return UserCanCreateObject(Session.User.Name, objectType, objectName);
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

		#region User Grants Management

		public void AddUserToGroup(string username, string group, bool asAdmin = false) {
			if (String.IsNullOrEmpty(@group))
				throw new ArgumentNullException("group");
			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");

			if (!UserCanAddToGroup(group))
				throw new SecurityException();

			UserManager.AddUserToGroup(username, group, asAdmin);
		}

		public void GrantToUserOn(ObjectName objectName, string grantee, Privileges privileges, bool withOption = false) {
			var obj = FindObject(objectName);
			if (obj == null)
				throw new ObjectNotFoundException(objectName);

			GrantToUserOn(obj.ObjectType, obj.FullName, grantee, privileges, withOption);
		}

		public void GrantToUserOn(DbObjectType objectType, ObjectName objectName, string grantee, Privileges privileges, bool withOption = false) {
			if (IsSystemUser(grantee))       // The @SYSTEM user does not need any other
				return;

			if (!ObjectExists(objectType, objectName))
				throw new ObjectNotFoundException(objectName);

			var granter = Session.User.Name;

			if (!UserHasGrantOption(objectType, objectName, privileges))
				throw new MissingPrivilegesException(granter, objectName, privileges);

			var grant = new Grant(privileges, objectName, objectType, granter, withOption);
			PrivilegeManager.GrantToUser(grantee, grant);
		}

		public void GrantToUserOnSchema(string schemaName, string grantee, Privileges privileges, bool withOption = false) {
			GrantToUserOn(DbObjectType.Schema, new ObjectName(schemaName), grantee, privileges, withOption);
		}

		public void GrantToGroupOn(DbObjectType objectType, ObjectName objectName, string groupName, Privileges privileges, bool withOption = false) {
			if (SystemGroups.IsSystemGroup(groupName))
				throw new InvalidOperationException("Cannot grant to a system group.");

			var granter = Session.User.Name;

			if (!UserCanManageGroups())
				throw new MissingPrivilegesException(granter, new ObjectName(groupName));

			if (!ObjectExists(objectType, objectName))
				throw new ObjectNotFoundException(objectName);

			var grant = new Grant(privileges, objectName, objectType, granter, withOption);
			PrivilegeManager.GrantToGroup(groupName, grant);
		}

		public void GrantTo(string groupOrUserName, DbObjectType objectType, ObjectName objectName, Privileges privileges, bool withOption = false) {
			if (UserManager.UserGroupExists(groupOrUserName)) {
				if (withOption)
					throw new SecurityException("User groups cannot be granted with grant option.");

				GrantToGroupOn(objectType, objectName, groupOrUserName, privileges);
			} else if (UserManager.UserExists(groupOrUserName)) {
				GrantToUserOn(objectType, objectName, groupOrUserName, privileges, withOption);
			} else {
				throw new SecurityException(String.Format("User or group '{0}' was not found.", groupOrUserName));
			}
		}

		public void RevokeAllGrantsOnTable(ObjectName objectName) {
			PrivilegeManager.RevokeAllGrantsOn(DbObjectType.Table, objectName);
		}

		public void RevokeAllGrantsOnView(ObjectName objectName) {
			PrivilegeManager.RevokeAllGrantsOn(DbObjectType.View, objectName);
		}

		public void GrantToUserOnTable(ObjectName tableName, string grantee, Privileges privileges) {
			GrantToUserOn(DbObjectType.Table, tableName, grantee, privileges);
		}

		#endregion

		#endregion

		#region Routines

		public bool IsSystemFunction(Invoke invoke, IRequest request) {
			var info = ResolveFunctionInfo(invoke, request);
			if (info == null)
				return false;

			return info.FunctionType != FunctionType.External &&
				   info.FunctionType != FunctionType.UserDefined;
		}

		public bool IsAggregateFunction(Invoke invoke, IRequest request) {
			var function = ResolveFunction(invoke, request);
			return function != null && function.FunctionType == FunctionType.Aggregate;
		}

		public IRoutine ResolveRoutine(Invoke invoke, IRequest request) {
			var routine = ResolveSystemRoutine(invoke, request);
			if (routine == null)
				routine = ResolveUserRoutine(invoke);

			return routine;
		}

		public IRoutine ResolveSystemRoutine(Invoke invoke, IRequest request) {
			// return request.SystemContext().ResolveRoutine(invoke, request);

			var resolvers = Session.Context.ResolveAllServices<IRoutineResolver>();
			foreach (var resolver in resolvers) {
				var routine = resolver.ResolveRoutine(invoke, request);
				if (routine != null)
					return routine;
			}

			return null;
		}

		public IRoutine ResolveUserRoutine(Invoke invoke) {
			return GetObject(DbObjectType.Routine, invoke.RoutineName, AccessType.Read) as IRoutine;
		}

		public IFunction ResolveFunction(Invoke invoke, IRequest request) {
			return ResolveRoutine(invoke, request) as IFunction;
		}

		public IFunction ResolveFunction(IRequest request, ObjectName functionName, params SqlExpression[] args) {
			var invoke = new Invoke(functionName, args);
			return ResolveFunction(invoke, request);
		}

		public FunctionInfo ResolveFunctionInfo(Invoke invoke, IRequest request) {
			return ResolveRoutineInfo(invoke, request) as FunctionInfo;
		}

		public RoutineInfo ResolveRoutineInfo(Invoke invoke, IRequest request) {
			var routine = ResolveRoutine(invoke, request);
			if (routine == null)
				return null;

			return routine.RoutineInfo;
		}

		public Field InvokeSystemFunction(IRequest request, string functionName, params SqlExpression[] args) {
			var resolvedName = new ObjectName(SystemSchema.SchemaName, functionName);
			var invoke = new Invoke(resolvedName, args);
			return InvokeFunction(request, invoke);
		}

		public Field InvokeFunction(IRequest request, Invoke invoke) {
			var result = invoke.Execute(request);
			return result.ReturnValue;
		}

		public Field InvokeFunction(IRequest request, ObjectName functionName, params SqlExpression[] args) {
			return InvokeFunction(request, new Invoke(functionName, args));
		}

		#endregion
	}
}
