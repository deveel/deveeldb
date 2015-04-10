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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public static class UserSessionExtensions {
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

		#region Objects

		public static IDbObject GetObject(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			// TODO: throw a specialized exception
			if (!session.UserCanAccessObject(session.User, objectType, objectName))
				throw new InvalidOperationException();

			return session.Transaction.GetObject(objectType, objectName);
		}

		public static void CreateObject(this IUserSession session, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!session.UserCanCreateObject(session.User, objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			session.Transaction.CreateObject(objectInfo);
		}

		public static bool ObjectExists(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			return session.Transaction.ObjectExists(objectType, objectName);
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

		public static bool TableExists(this IUserSession session, ObjectName tableName) {
			return session.ObjectExists(DbObjectType.Table, tableName);
		}

		public static ITable GetTable(this IUserSession session, ObjectName tableName) {
			var table = session.GetObject(DbObjectType.Table, tableName) as ITable;
			session.Transaction.GetTableManager().SelectTable(tableName);
			return table;
		}

		public static void CreateTable(this IUserSession session, TableInfo tableInfo) {
			session.CreateObject(tableInfo);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns, string constraintName) {
			AddPrimaryKey(session, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this IUserSession session, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferred, string constraintName) {
			// TODO: throw a specialized exception
			if (!session.UserCanAlterTable(session.User, tableName))
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
			if (!session.UserCanAlterTable(session.User, table))
				throw new InvalidOperationException();

			session.Transaction.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
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

		#region Sequences

		/// <summary>
		/// Requests the sequence generator for the next value.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <returns></returns>
		public static SqlNumber NextSequenceValue(this IUserSession session, string name) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			return session.Transaction.NextValue(seqName);
		}

		/// <summary>
		/// Returns the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// The value returned is the same value returned by <see cref="NextSequenceValue"/>.
		/// <para>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If no value was returned by <see cref="NextSequenceValue"/>.
		/// </exception>
		public static SqlNumber LastSequenceValue(this IUserSession session, string name) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			return session.Transaction.LastValue(seqName);
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public static void SetSequenceValue(this IUserSession session, string name, SqlNumber value) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			session.Transaction.SetValue(seqName, value);
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
			return UserBelongsToGroup(session, session.User, groupName);
		}

		public static bool UserBelongsToGroup(this IUserSession session, User user, string groupName) {
			using (var context = new SessionQueryContext(session)) {
				return context.UserBelongsToGroup(user.Name, groupName);
			}
		}

		public static bool UserBelongsToSecureGroup(this IUserSession session) {
			return UserBelongsToSecureGroup(session, session.User);
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