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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Transactions;

namespace Deveel.Data {
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
			return GetObject(session, objectType, objectName, AccessType.ReadWrite);
		}

		public static IDbObject GetObject(this IUserSession session, DbObjectType objectType, ObjectName objectName, AccessType accessType) {
			var obj = session.Transaction.GetObject(objectType, objectName);
			if (obj != null)
				session.Access(obj, accessType);

			return obj;
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

		public static void DropObject(this IUserSession session, DbObjectType objectType, ObjectName objectName) {
			session.Transaction.DropObject(objectType, objectName);
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
			return session.GetObject(DbObjectType.Table, tableName) as ITable;
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

		#region Constraints

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

		public static void AddCheck(this IUserSession session, ObjectName tableName, SqlExpression expression, ConstraintDeferrability deferrability,
			string constraintName) {
			session.Transaction.AddCheck(tableName, expression, deferrability, constraintName);
		}

		public static void DropAllTableConstraints(this IUserSession session, ObjectName tableName) {
			session.Transaction.DropAllTableConstraints(tableName);
		}

		public static int DropTableConstraint(this IUserSession session, ObjectName tableName, string constraintName) {
			return session.Transaction.DropTableConstraint(tableName, constraintName);
		}

		public static bool DropTablePrimaryKey(this IUserSession session, ObjectName tableName, string constraintName) {
			return session.Transaction.DropTablePrimaryKey(tableName, constraintName);
		}

		public static ObjectName[] QueryTablesRelationallyLinkedTo(this IUserSession session, ObjectName tableName) {
			return session.Transaction.QueryTablesRelationallyLinkedTo(tableName);
		}

		public static ConstraintInfo[] QueryTableCheckExpressions(this IUserSession session, ObjectName tableName) {
			return session.Transaction.QueryTableCheckExpressions(tableName);
		}

		public static ConstraintInfo QueryTablePrimaryKey(this IUserSession session, ObjectName tableName) {
			return session.Transaction.QueryTablePrimaryKey(tableName);
		}

		public static ConstraintInfo[] QueryTableUniqueKeys(this IUserSession session, ObjectName tableName) {
			return session.Transaction.QueryTableUniqueKeys(tableName);
		}

		public static ConstraintInfo[] QueryTableImportedForeignKeys(this IUserSession session, ObjectName refTableName) {
			return session.Transaction.QueryTableImportedForeignKeys(refTableName);
		}

		public static ConstraintInfo[] QueryTableForeignKeys(this IUserSession session, ObjectName tableName) {
			return session.Transaction.QueryTableForeignKeys(tableName);
		}

		public static void CheckConstraintViolations(this IUserSession session, ObjectName tableName) {
			throw new NotImplementedException();
		}

		#endregion

		#endregion

		#region Locks

		public static void Access(this IUserSession session, IDbObject obj, AccessType accessType) {
			session.Access(new [] {obj}, accessType);
		}

		#endregion
	}
}