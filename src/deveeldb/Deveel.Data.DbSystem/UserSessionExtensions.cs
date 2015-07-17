﻿// 
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
using System.Collections.Generic;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	public static class UserSessionExtensions {
		#region Variables

		public static bool AutoCommit(this IUserSession session) {
			return session.Transaction.AutoCommit();
		}

		public static void AutoCommit(this IUserSession session, bool value) {
			session.Transaction.AutoCommit(value);
		}

		public static void CurrentSchema(this IUserSession session, string value) {
			session.Transaction.CurrentSchema(value);
		}

		public static string CurrentSchema(this IUserSession session) {
			return session.Transaction.CurrentSchema();
		}

		public static bool IgnoreIdentifiersCase(this IUserSession session) {
			return session.Transaction.IgnoreIdentifiersCase();
		}

		#endregion

		#region Objects

		public static IDbObject GetObject(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			// TODO: throw a specialized exception
			if (!session.UserCanAccessObject(session.SessionInfo.User, objectType, objectName))
				throw new InvalidOperationException();

			return session.Transaction.GetObject(objectType, objectName);
		}

		public static void CreateObject(this IUserSession session, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!session.UserCanCreateObject(session.SessionInfo.User, objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			session.Transaction.CreateObject(objectInfo);
		}

		public static bool ObjectExists(this IUserSession session, ObjectName objectName) {
			return session.Transaction.ObjectExists(objectName);
		}

		public static bool ObjectExists(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			return session.Transaction.ObjectExists(objectType, objectName);
		}

		public static ObjectName ResolveObjectName(this IUserSession session, string name) {
			return session.ResolveObjectName(new ObjectName(new ObjectName(session.CurrentSchema), name));
		}

		public static ObjectName ResolveObjectName(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			return session.Transaction.ResolveObjectName(objectType, objectName);
		}

		public static ObjectName ResolveObjectName(this IUserSession session, ObjectName objectName) {
			return session.Transaction.ResolveObjectName(objectName);
		}

		#endregion

		#region Sequences

		public static ISequence GetSequence(this IUserSession session, ObjectName sequenceName) {
			return session.GetObject(DbObjectType.Sequence, sequenceName) as ISequence;
		}

		#endregion

		#region Schemata

		public static void CreateSchema(this IUserSession session, string name, string type) {
			session.CreateObject(new SchemaInfo(name, type));
		}

		#endregion

		#region Tables

		public static ITable GetCachedTable(this IUserSession session, ObjectName tableName) {
			if (session.TableCache == null)
				return null;

			return session.TableCache.Get(tableName) as ITable;
		}

		public static void CacheTable(this IUserSession session, ObjectName tableName, ITable table) {
			if (session.TableCache == null)
				return;

			session.TableCache.Set(tableName, table);
		}

		public static ObjectName ResolveTableName(this IUserSession session, ObjectName tableName) {
			return session.Transaction.ResolveTableName(tableName);
		}

		public static bool TableExists(this IUserSession session, ObjectName tableName) {
			return session.ObjectExists(DbObjectType.Table, tableName);
		}

		public static ITable GetTable(this IUserSession session, ObjectName tableName) {
			var table = session.Transaction.GetTable(tableName);

			if (table == null)
				return null;

			var dtable = session.GetCachedTable(tableName);
			if (dtable == null) {
				dtable = new DataTable(session, table);
				session.CacheTable(tableName, dtable);
			}

			return dtable;
		}

		public static IMutableTable GetMutableTable(this IUserSession session, ObjectName tableName) {
			return session.GetTable(tableName) as IMutableTable;
		}

		public static TableInfo GetTableInfo(this IUserSession session, ObjectName tableName) {
			return session.Transaction.GetTableInfo(tableName);
		}

		public static string GetTableType(this IUserSession session, ObjectName tableName) {
			return session.Transaction.GetTableType(tableName);
		}

		public static void CreateTable(this IUserSession session, TableInfo tableInfo) {
			CreateTable(session, tableInfo, false);
		}

		public static void CreateTable(this IUserSession session, TableInfo tableInfo, bool temporary) {
			session.Transaction.CreateTable(tableInfo, temporary);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string columnName) {
			AddPrimaryKey(session, tableName, new[] {columnName});
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns) {
			AddPrimaryKey(session, tableName, columns, null);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string columnName, string constraintName) {
			AddPrimaryKey(session, tableName, new[] {columnName}, constraintName);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns, string constraintName) {
			AddPrimaryKey(session, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string columnName,
			ConstraintDeferrability deferred, string constraintName) {
			AddPrimaryKey(session, tableName, new[] {columnName}, deferred, constraintName);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferred, string constraintName) {
			// TODO: throw a specialized exception
			if (!session.UserCanAlterTable(session.SessionInfo.User, tableName))
				throw new InvalidOperationException();

			session.Transaction.AddPrimaryKey(tableName, columns, deferred, constraintName);
		}

		

		public static void AddForeignKey(this IUserSession session, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns, String constraintName) {
			AddForeignKey(session, table, columns, refTable, refColumns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this IUserSession session, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns, ConstraintDeferrability deferred, String constraintName) {
			session.AddForeignKey(table, columns, refTable, refColumns, ForeignKeyAction.NoAction, ForeignKeyAction.NoAction, deferred, constraintName);
		}

		public static void AddForeignKey(this IUserSession session, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, String constraintName) {
			AddForeignKey(session, table, columns, refTable, refColumns, deleteRule, updateRule, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this IUserSession session, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred, String constraintName) {
			// TODO: throw a specialized exception
			if (!session.UserCanAlterTable(session.SessionInfo.User, table))
				throw new InvalidOperationException();

			session.Transaction.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static int DeleteFrom(this IUserSession session, ObjectName tableName, ITable deleteSet) {
			return DeleteFrom(session, tableName, deleteSet, -1);
		}

		public static int DeleteFrom(this IUserSession session, ObjectName tableName, ITable deleteSet, int limit) {
			var table = session.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			return table.Delete(deleteSet, limit);
		}

		public static int DeleteFrom(this IUserSession session, ObjectName tableName, SqlExpression whereExpression, int limit) {
			var table = session.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);


			var expression = new SqlQueryExpression(new List<SelectColumn> {SelectColumn.Glob("*")});
			expression.FromClause.AddTable(tableName.Name);
			expression.WhereExpression = whereExpression;

			ITable deleteSet;

			using (var context = new SystemQueryContext(session.Transaction, session.CurrentSchema)) {
				var planExpression = expression.Evaluate(context, null);
				var plan = (SqlQueryObject) ((SqlConstantExpression) planExpression).Value.Value;
				deleteSet = plan.QueryPlan.Evaluate(context);
			}

			return session.DeleteFrom(tableName, deleteSet, limit);
		}

		#endregion

		#region Views

		public static View GetView(this IUserSession session, ObjectName viewName) {
			return session.GetObject(DbObjectType.View, viewName) as View;
		}

		public static IQueryPlanNode GetViewQueryPlan(this IUserSession session, ObjectName viewName) {
			var view = session.GetView(viewName);
			return view == null ? null : view.QueryPlan;
		}

		#endregion

		#region Types

		public static UserType GetUserType(this IUserSession session, ObjectName typeName) {
			return (UserType) session.GetObject(DbObjectType.Type, typeName);
		}

		#endregion

		#region Locks

		public static void ExclusiveLock(this IUserSession session) {
			session.Lock(LockingMode.Exclusive);
		}

		public static void Lock(this IUserSession session, LockingMode mode) {
			var lockable = new ILockable[] { session.Transaction };
			session.Lock(lockable, lockable, LockingMode.Exclusive);
		}

		#endregion

		#region Triggers

		public static void FireTrigger(this IUserSession session, TriggerContext context) {
			var manager = session.Transaction.GetTriggerManager();
			if (manager == null)
				return;

			var eventInfo = new TriggerEventInfo(context.Table.TableInfo.TableName, context.EventType);
			var triggers = manager.FindTriggers(eventInfo);
		}

		#endregion

		#region Security

		public static bool UserExists(this IUserSession session, string userName) {
			var table = session.GetTable(SystemSchema.UserTableName);
			return table.Exists(0, DataObject.String(userName));
		}

		public static bool UserCanCreateObject(this IUserSession transaction, User user, DbObjectType objectType,
			ObjectName objectName) {
			return transaction.UserHasPrivilege(user, objectType, objectName, Privileges.Create);
		}

		public static bool UserCanAccessObject(this IUserSession transaction, User user, DbObjectType objectType, ObjectName objectName) {
			return transaction.UserHasPrivilege(user, objectType, objectName, Privileges.Select);
		}

		public static bool UserCanAlterObject(this IUserSession transaction, User user, DbObjectType objectType,
			ObjectName objectName) {
			return transaction.UserHasPrivilege(user, objectType, objectName, Privileges.Alter);
		}

		public static bool UserHasPrivilege(this IUserSession session, DbObjectType objectType,
			ObjectName objectName, Privileges privilege) {
			return UserHasPrivilege(session, session.SessionInfo.User, objectType, objectName, privilege);
		}

		public static bool UserHasPrivilege(this IUserSession session, User user, DbObjectType objectType,
			ObjectName objectName, Privileges privilege) {
			if (user.IsSystem)
				return true;

			if (session.UserBelongsToSecureGroup(user))
				return true;

			Privileges grant;
			if (!user.TryGetObjectGrant(objectName, out grant)) {
				grant = session.GetUserGrant(user.Name, objectType, objectName);
				user.CacheObjectGrant(objectName, grant);
			}

			return (grant & privilege) != 0;
		}

		public static bool UserBelongsToGroup(this IUserSession session, string groupName) {
			return UserBelongsToGroup(session, session.SessionInfo.User, groupName);
		}

		public static bool UserBelongsToGroup(this IUserSession session, User user, string groupName) {
			using (var context = new SessionQueryContext(session)) {
				return context.UserBelongsToGroup(user.Name, groupName);
			}
		}

		public static bool UserBelongsToSecureGroup(this IUserSession session) {
			return UserBelongsToSecureGroup(session, session.SessionInfo.User);
		}

		public static bool UserBelongsToSecureGroup(this IUserSession session, User user) {
			return session.UserBelongsToGroup(user, SystemGroupNames.SecureGroup);
		}

		public static Privileges GetUserGrant(this IUserSession session, string userName, DbObjectType objectType,
			ObjectName objectName) {
			using (var context = new SessionQueryContext(session)) {
				return context.GetUserGrants(userName, objectType, objectName);
			}
		}

		public static bool UserCanReferenceTable(this IUserSession transaction, User user, ObjectName tableName) {
			return transaction.UserHasPrivilege(user, DbObjectType.Table, tableName, Privileges.References);
		}

		public static bool UserCanAlterTable(this IUserSession transaction, User user, ObjectName tableName) {
			return transaction.UserCanAlterObject(user, DbObjectType.Table, tableName);
		}


		#endregion
	}
}