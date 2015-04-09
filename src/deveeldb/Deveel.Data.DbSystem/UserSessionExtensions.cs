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

using Deveel.Data.Sql;
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
			if (!session.Transaction.UserCanAccessObject(session.User, objectType, objectName))
				throw new InvalidOperationException();

			return session.Transaction.GetObject(objectType, objectName);
		}

		public static void CreateObject(this IUserSession session, IObjectInfo objectInfo) {
			// TODO: throw a specialized exception
			if (!session.Transaction.UserCanCreateObject(session.User, objectInfo.ObjectType, objectInfo.FullName))
				throw new InvalidOperationException();

			session.Transaction.CreateObject(objectInfo);
		}

		#endregion

		#region Sequences

		public static ISequence GetSequence(this IUserSession session, ObjectName sequenceName) {
			return session.GetObject(DbObjectType.Sequence, sequenceName) as ISequence;
		}

		#endregion

		#region Tables

		public static ITable GetTable(this IUserSession session, ObjectName tableName) {
			return session.GetObject(DbObjectType.Table, tableName) as ITable;
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
			if (!session.Transaction.UserCanAlterTable(session.User, tableName))
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
			if (!session.Transaction.UserCanAlterTable(session.User, table))
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
	}
}