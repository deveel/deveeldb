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
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Transactions;

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

		public static void IgnoreIdentifiersCase(this IUserSession session, bool value) {
			session.Transaction.IgnoreIdentifiersCase(value);
		}

		public static QueryParameterStyle ParameterStyle(this IUserSession session) {
			return session.Transaction.ParameterStyle();
		}

		public static void ParameterStyle(this IUserSession session, QueryParameterStyle value) {
			session.Transaction.ParameterStyle(value);
		}

		#endregion

		#region Objects

		public static IDbObject GetObject(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			return session.Transaction.GetObject(objectType, objectName);
		}

		public static void CreateObject(this IUserSession session, IObjectInfo objectInfo) {
			session.Transaction.CreateObject(objectInfo);
		}

		public static void AlterObject(this IUserSession session, IObjectInfo objectInfo) {
			session.Transaction.AlterObject(objectInfo);
		}

		public static bool ObjectExists(this IUserSession session, ObjectName objectName) {
			return session.Transaction.ObjectExists(objectName);
		}

		public static bool ObjectExists(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			return session.Transaction.ObjectExists(objectType, objectName);
		}

		public static IDbObject FindObject(this IUserSession session, ObjectName objectName) {
			return session.Transaction.FindObject(objectName);
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

		#region Tables


		public static ObjectName ResolveTableName(this IUserSession session, ObjectName tableName) {
			return session.Transaction.ResolveTableName(tableName);
		}

		public static ITable GetTable(this IUserSession session, ObjectName tableName) {
			tableName = session.ResolveTableName(tableName);
			return session.Transaction.GetTable(tableName);
		}

		public static TableInfo GetTableInfo(this IUserSession session, ObjectName tableName) {
			return session.Transaction.GetTableInfo(tableName);
		}

		public static string GetTableType(this IUserSession session, ObjectName tableName) {
			return session.Transaction.GetTableType(tableName);
		}

		public static void CreateTable(this IUserSession session, TableInfo tableInfo, bool temporary) {
			session.Transaction.CreateTable(tableInfo, temporary);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			session.Transaction.AddPrimaryKey(tableName, columns, deferred, constraintName);
		}

		public static void AddForeignKey(this IUserSession session, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred, String constraintName) {
			session.Transaction.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static void AddUniqueKey(this IUserSession session, ObjectName tableName, string[] columns, ConstraintDeferrability deferrability, string constraintName) {
			session.Transaction.AddUniqueKey(tableName, columns, deferrability, constraintName);
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

		public static IEnumerable<Trigger> FindTriggers(this IUserSession session, ObjectName tableName, TriggerEventType eventType) {
			var manager = session.Transaction.GetTriggerManager();
			if (manager == null)
				return new Trigger[0];

			var eventInfo = new TriggerEventInfo(tableName, eventType);
			return manager.FindTriggers(eventInfo);
		} 

		#endregion
	}
}