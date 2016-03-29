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
using System.Linq;

using Deveel.Data.Caching;
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

		private ICache userRolesCache;
		private ICache privsCache;

		protected abstract ISession Session { get; }

		private ISession SystemSession {
			get {
				if (Session is SystemSession)
					return Session;

				return new SystemSession(Session.Transaction, Session.CurrentSchema);
			}
		}

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

		private ICache PrivilegesCache {
			get {
				if (privsCache == null)
					privsCache = Session.Context.ResolveService<ICache>("Privileges");
				if (privsCache == null)
					privsCache = new MemoryCache();

				return privsCache;
			}
		}

		private ICache UserRolesCache {
			get {
				if (userRolesCache == null)
					userRolesCache = Session.Context.ResolveService<ICache>("UserRoles");
				if (userRolesCache == null)
					userRolesCache = new MemoryCache();

				return userRolesCache;
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
			Session.Transaction.CreateObject(objectInfo);
		}

		public virtual void AlterObject(IObjectInfo objectInfo) {
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
			if (!Session.User.CanDrop(objectType, objectName))
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
			if (!Session.User.CanManageSchema())
				throw new MissingPrivilegesException(Session.User.Name, new ObjectName(name), Privileges.Create);

			CreateObject(new SchemaInfo(name, type));
		}

		public void DropSchema(string schemaName) {
			if (!Session.User.CanDropSchema(schemaName))
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

		public IMutableTable GetMutableTable(ObjectName tableName) {
			return GetTable(tableName) as IMutableTable;
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
			if (!Session.User.CanAlterTable(tableName))
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

			if (!Session.User.CanCreateTable(tableName))
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

		#region Role Management

		public void CreateRole(string roleName) {
			SystemSession.Access.UserManager.CreateRole(roleName);
		}

		public bool RoleExists(string roleName) {
			return SystemSession.Access.UserManager.RoleExists(roleName);
		}

		public bool DropRole(string roleName) {
			try {
				return SystemSession.Access.UserManager.DropRole(roleName);
			} finally {
				RevokeAllGrantsFrom(roleName);
			}
		}

		public void SetRoleAdmin(string roleName, string userName) {
			SystemSession.Access.UserManager.SetRoleAdmin(roleName, userName);
		}

		#endregion

		#region User Management

		public void SetUserStatus(string username, UserStatus status) {
			SystemSession.Access.UserManager.SetUserStatus(username, status);
		}

		public UserStatus GetUserStatus(string userName) {
			return SystemSession.Access.UserManager.GetUserStatus(userName);
		}

		public bool UserExists(string userName) {
			return SystemSession.Access.UserManager.UserExists(userName);
		}

		public void CreatePublicUser() {
			if (!Session.User.IsSystem)
				throw new InvalidOperationException("The @PUBLIC user can be created only by the SYSTEM");

			var userName = User.PublicName;
			var userId = new UserIdentification(KnownUserIdentifications.ClearText, "###");
			var userInfo = new UserInfo(userName, userId);

			SystemSession.Access.UserManager.CreateUser(userInfo);
		}

		public void CreateUser(string userName, string password) {
			CreateUser(userName, KnownUserIdentifications.ClearText, password);
		}

		public void CreateUser(string userName, string identification, string token) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(identification))
				throw new ArgumentNullException("identification");
			if (String.IsNullOrEmpty(token))
				throw new ArgumentNullException("token");

			if (String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(
					String.Format("User name '{0}' is reserved and cannot be registered.", User.PublicName), "userName");

			if (userName.Length <= 1)
				throw new ArgumentException("User name must be at least one character.");
			if (token.Length <= 1)
				throw new ArgumentException("The password must be at least one character.");

			var c = userName[0];
			if (c == '#' || c == '@' || c == '$' || c == '&')
				throw new ArgumentException(
					String.Format("User name '{0}' is invalid: cannot start with '{1}' character.", userName, c), "userName");

			var identifier = FindIdentifier(identification);
			if (identifier == null)
				throw new ArgumentException(String.Format("User identification method '{0}' cannot be found", identification));

			var userId = identifier.CreateIdentification(token);
			var userInfo = new UserInfo(userName, userId);

			SystemSession.Access.UserManager.CreateUser(userInfo);
		}

		public void AlterUserPassword(string username, string token) {
			AlterUserPassword(username, KnownUserIdentifications.ClearText, token);
		}

		public void AlterUserPassword(string username, string identification, string token) {
			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");
			if (String.IsNullOrEmpty(identification))
				throw new ArgumentNullException("identification");

			var identifier = FindIdentifier(identification);
			if (identifier == null)
				throw new ArgumentException(String.Format("User identification method '{0}' cannot be found", identification));

			var userId = identifier.CreateIdentification(token);
			var userInfo = new UserInfo(username, userId);

			SystemSession.Access.UserManager.AlterUser(userInfo);
		}

		public bool DeleteUser(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			try {
				return SystemSession.Access.UserManager.DropUser(userName);
			} finally {
				RevokeAllGrantsFrom(userName);
			}
		}

		private IUserIdentifier FindIdentifier(string name) {
			return Session.Context.ResolveAllServices<IUserIdentifier>()
				.FirstOrDefault(x => x.Name == name);
		}

		public bool Authenticate(string username, string password) {
			try {
				if (String.IsNullOrEmpty(username))
					throw new ArgumentNullException("username");
				if (String.IsNullOrEmpty(password))
					throw new ArgumentNullException("password");

				var userInfo = SystemSession.Access.UserManager.GetUser(username);

				if (userInfo == null)
					return false;

				var userId = userInfo.Identification;
				var identifier = FindIdentifier(userId.Method);

				if (identifier == null)
					throw new SecurityException(String.Format("The user '{0}' was identified by '{1}' but the identifier cannot be found in the context.", userInfo.Name, userId.Method));

				if (!identifier.VerifyIdentification(password, userId))
					return false;

				// Successfully authenticated...
				return true;
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new SecurityException("Could not authenticate user.", ex);
			}
		}

		#endregion


		#region Privilege Query

		public void SetUserRoles(string userName, string[] roleNames) {
			try {
				var userRoles = UserManager.GetUserRoles(userName);
				foreach (var userGroup in userRoles) {
					UserManager.RemoveUserFromRole(userName, userGroup);
				}

				foreach (var userGroup in roleNames) {
					UserManager.AddUserToRole(userName, userGroup, false);
				}
			} finally {
				UserRolesCache.Remove(userName);
			}
		}

		public Role[] GetUserRoles(string username) {
			string[] roles;
			object cached;
			if (!UserRolesCache.TryGet(username, out cached)) {
				roles = UserManager.GetUserRoles(username);

				UserRolesCache.Set(username, roles);
			} else {
				roles = (string[]) cached;
			}

			if (roles == null || roles.Length == 0)
				return new Role[0];

			return roles.Select(x => new Role(Session, x)).ToArray();
		}

		public bool UserIsInRole(string username, string roleName) {
			return UserManager.IsUserInRole(username, roleName);
		}

		public bool UserIsRoleAdmin(string userName, string roleName) {
			return UserManager.IsUserRoleAdmin(userName, roleName);
		}

		public void AddUserToRole(string username, string role, bool asAdmin = false) {
			if (String.IsNullOrEmpty(role))
				throw new ArgumentNullException("role");
			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");

			try {
				UserManager.AddUserToRole(username, role, asAdmin);
			} finally {
				UserRolesCache.Remove(username);
			}
		}

		private Privileges GetPrivileges(string grantee, DbObjectType objectType, ObjectName objectName, bool withOption) {
			object privsObj;
			Privileges privs;

			var key = new GrantCacheKey(grantee, objectType, objectName.FullName, true, true);

			if (PrivilegesCache.TryGet(key, out privsObj)) {
				privs = (Privileges)privsObj;
			} else {
				var grants = PrivilegeManager.GetGrants(grantee, true);
				foreach (var g in grants) {
					PrivilegesCache.Set(new GrantCacheKey(g.Grantee, g.ObjectType, g.ObjectName.FullName, g.WithOption, true), g.Privileges);
				}

				var grantOptions = grants.Where(x => x.WithOption &&
															  x.ObjectType == objectType &&
															  x.ObjectName.Equals(objectName));

				privs = Privileges.None;
				foreach (var grant in grantOptions) {
					privs |= grant.Privileges;
				}
			}

			return privs;
		}

		public bool HasGrantOption(string granter, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (User.IsSystemUserName(granter))
				return true;

			var privs = GetPrivileges(granter, objectType, objectName, true);
			return (privs & privileges) != 0;
		}

		public bool UserHasPrivilege(string grantee, DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (User.IsSystemUserName(grantee))
				return true;

			var privs = GetPrivileges(grantee, objectType, objectName, false);
			return (privs & privileges) != 0;
		}

		#region GrantCacheKey

		class GrantCacheKey : IEquatable<GrantCacheKey> {
			private readonly string userName;
			private readonly DbObjectType objectType;
			private readonly string objectName;
			private readonly int options;

			public GrantCacheKey(string userName, DbObjectType objectType, string objectName, bool withOption, bool withPublic) {
				this.userName = userName;
				this.objectType = objectType;
				this.objectName = objectName;

				options = 0;
				if (withOption)
					options++;
				if (withPublic)
					options++;
			}

			public override bool Equals(object obj) {
				var other = obj as GrantCacheKey;
				return Equals(other);
			}

			public override int GetHashCode() {
				return unchecked(((userName.GetHashCode() * objectName.GetHashCode()) ^ (int)objectType) + options);
			}

			public bool Equals(GrantCacheKey other) {
				if (other == null)
					return false;

				if (!String.Equals(userName, other.userName, StringComparison.OrdinalIgnoreCase))
					return false;

				if (objectType != other.objectType)
					return false;

				if (!String.Equals(objectName, other.objectName, StringComparison.OrdinalIgnoreCase))
					return false;

				if (options != other.options)
					return false;

				return true;
			}
		}

		#endregion

		#endregion

		#region Grants Management

		public void GrantOn(DbObjectType objectType, ObjectName objectName, string grantee, Privileges privileges, bool withOption = false) {
			try {
				var granter = Session.User.Name;

				var grant = new Grant(privileges, objectName, objectType, grantee, granter, withOption);
				PrivilegeManager.Grant(grant);
			} finally {
				PrivilegesCache.Remove(new GrantCacheKey(grantee, objectType, objectName.FullName, withOption, false));
			}
		}

		public void GrantOnSchema(string schemaName, string grantee, Privileges privileges, bool withOption = false) {
			GrantOn(DbObjectType.Schema, new ObjectName(schemaName), grantee, privileges, withOption);
		}

		public void GrantTo(string grantee, DbObjectType objectType, ObjectName objectName, Privileges privileges, bool withOption = false) {
			if (!UserManager.UserExists(grantee) &&
				!UserManager.RoleExists(grantee))
				throw new SecurityException(String.Format("User or role '{0}' was not found.", grantee));

			if (UserManager.RoleExists(grantee) &&
				withOption)
				throw new SecurityException("Roles cannot be granted with grant option.");

			GrantOn(objectType, objectName, grantee, privileges);
		}

		public void RevokeAllGrantsOn(DbObjectType objectType, ObjectName objectName) {
			var grants = PrivilegeManager.GetGrantsOn(objectType, objectName);

			try {
				foreach (var grant in grants) {
					PrivilegeManager.Revoke(grant);
				}
			} finally {
				foreach (var grant in grants) {
					PrivilegesCache.Remove(new GrantCacheKey(grant.Grantee, grant.ObjectType, grant.ObjectName.FullName,
						grant.WithOption, false));
				}
			}
		}

		public void RevokeAllGrantsOnTable(ObjectName objectName) {
			RevokeAllGrantsOn(DbObjectType.Table, objectName);
		}

		public void RevokeAllGrantsOnView(ObjectName objectName) {
			RevokeAllGrantsOn(DbObjectType.View, objectName);
		}

		private void RevokeAllGrantsFrom(string grantee) {
			var grants = PrivilegeManager.GetGrants(grantee, false);

			try {
				foreach (var grant in grants) {
					PrivilegeManager.Revoke(grant);
				}
			} finally {
				foreach (var grant in grants) {
					PrivilegesCache.Remove(new GrantCacheKey(grant.Grantee, grant.ObjectType, grant.ObjectName.FullName,
						grant.WithOption, false));
				}
			}
		}

		public void GrantOnTable(ObjectName tableName, string grantee, Privileges privileges, bool withOption = false) {
			GrantOn(DbObjectType.Table, tableName, grantee, privileges, withOption);
		}

		public void Revoke(DbObjectType objectType, ObjectName objectName, string grantee, Privileges privileges,
			bool grantOption = false) {
			try {
				var revoker = Session.User.Name;
				var grant = new Grant(privileges, objectName, objectType, grantee, revoker, grantOption);
				SystemSession.Access.PrivilegeManager.Revoke(grant);
			} finally {
				var key = new GrantCacheKey(grantee, objectType, objectName.FullName, grantOption, false);
				PrivilegesCache.Remove(key);
			}
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

		public IProcedure ResolveProcedure(Invoke invoke) {
			return ResolveUserRoutine(invoke) as IProcedure;
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

		public bool RoutineExists(ObjectName routineName) {
			return ObjectExists(DbObjectType.Routine, routineName);
		}

		public bool DeleteRoutine(ObjectName routineName) {
			return SystemSession.Access.DropObject(DbObjectType.Routine, routineName);
		}

		public void CreateRoutine(RoutineInfo routineInfo) {
			SystemSession.Access.CreateObject(routineInfo);
		}

		#endregion
	}
}
