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
using System.Linq;

using Deveel.Data.Caching;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Sql.Tables {
	public static partial class QueryExtensions {
		public static ObjectName ResolveTableName(this IQuery query, ObjectName tableName) {
			return query.Session.ResolveTableName(tableName);
		}

		public static ObjectName ResolveTableName(this IQuery query, string name) {
			var schema = query.CurrentSchema();
			if (String.IsNullOrEmpty(schema))
				throw new InvalidOperationException("Default schema not specified in the query.");

			var objSchemaName = query.ResolveSchemaName(schema);
			if (objSchemaName == null)
				throw new InvalidOperationException(
					String.Format("The default schema of the session '{0}' is not defined in the database.", schema));

			var objName = ObjectName.Parse(name);
			if (objName.Parent == null)
				objName = new ObjectName(objSchemaName, objName.Name);

			return query.ResolveTableName(objName);
		}

		public static bool TableExists(this IQuery query, ObjectName tableName) {
			return query.ObjectExists(DbObjectType.Table, tableName);
		}

		public static void CreateTable(this IQuery query, TableInfo tableInfo) {
			CreateTable(query, tableInfo, false);
		}

		public static void CreateTable(this IQuery query, TableInfo tableInfo, bool onlyIfNotExists) {
			CreateTable(query, tableInfo, onlyIfNotExists, false);
		}

		public static void CreateTable(this IQuery query, TableInfo tableInfo, bool onlyIfNotExists, bool temporary) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!query.UserCanCreateTable(tableName))
				throw new MissingPrivilegesException(query.User().Name, tableName, Privileges.Create);

			if (query.TableExists(tableName)) {
				if (!onlyIfNotExists)
					throw new InvalidOperationException(
						String.Format("The table {0} already exists and the IF NOT EXISTS clause was not specified.", tableName));

				return;
			}

			query.Session.CreateTable(tableInfo, temporary);

			using (var systemContext = query.Direct()) {
				systemContext.GrantToUserOnTable(tableInfo.TableName, query.User().Name, Privileges.TableAll);
			}
		}

		public static ITableQueryInfo GetTableQueryInfo(this IQuery context, ObjectName tableName, ObjectName alias) {
			return context.Session.GetTableQueryInfo(tableName, alias);
		}

		internal static void CreateSystemTable(this IQuery query, TableInfo tableInfo) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!query.UserCanCreateTable(tableName))
				throw new MissingPrivilegesException(query.User().Name, tableName, Privileges.Create);

			query.Session.CreateTable(tableInfo, false);
		}

		public static void DropTables(this IQuery query, IEnumerable<ObjectName> tableNames) {
			DropTables(query, tableNames, false);
		}

		public static void DropTables(this IQuery query, IEnumerable<ObjectName> tableNames, bool onlyIfExists) {
			var tableNameList = tableNames.ToList();
			foreach (var tableName in tableNameList) {
				if (!query.UserCanDropObject(DbObjectType.Table, tableName))
					throw new MissingPrivilegesException(query.UserName(), tableName, Privileges.Drop);
			}

			// Check there are no referential links to any tables being dropped
			foreach (var tableName in tableNameList) {
				var refs = query.GetTableImportedForeignKeys(tableName);

				foreach (var reference in refs) {
					// If the key table isn't being dropped then error
					if (!tableNameList.Contains(reference.TableName)) {
						throw new ConstraintViolationException(SqlModelErrorCodes.DropTableViolation,
							String.Format("Constraint violation ({0}) dropping table '{1}' because of referential link from '{2}'",
								reference.ConstraintName, tableName, reference.TableName));
					}
				}
			}

			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!onlyIfExists) {
				// For each table to drop.
				foreach (var tableName in tableNameList) {
					// If table doesn't exist, throw an error
					if (!query.TableExists(tableName)) {
						throw new InvalidOperationException(String.Format("The table '{0}' does not exist and cannot be dropped.",
							tableName));
					}
				}
			}

			foreach (var tname in tableNameList) {
				// Does the table already exist?
				if (query.TableExists(tname)) {
					// Drop table in the transaction
					query.DropObject(DbObjectType.Table, tname);

					// Revoke all the grants on the table
					query.RevokeAllGrantsOnTable(tname);

					// Drop all constraints from the schema
					query.DropAllTableConstraints(tname);
				}
			}
		}

		public static void DropTable(this IQuery query, ObjectName tableName) {
			DropTable(query, tableName, false);
		}

		public static void DropTable(this IQuery query, ObjectName tableName, bool onlyIfExists) {
			query.DropTables(new[] { tableName }, onlyIfExists);
		}

		public static void AlterTable(this IQuery query, TableInfo tableInfo) {
			query.AlterObject(tableInfo);
		}

		private static ICache TableCache(this IQuery query) {
			return query.Context.ResolveService<ICache>("TableCache");
		}

		public static ITable GetTable(this IQuery query, ObjectName tableName) {
			var table = query.GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = query.Session.GetTable(tableName);
				if (table != null) {
					table = new UserContextTable(query, table);
					query.CacheTable(tableName.FullName, table);
				}
			}

			return table;
		}

		public static IMutableTable GetMutableTable(this IQuery query, ObjectName tableName) {
			return query.GetTable(tableName) as IMutableTable;
		}

		public static ITable GetCachedTable(this IQuery query, string cacheKey) {
			var tableCache = query.TableCache();
			if (tableCache == null)
				return null;

			object obj;
			if (!tableCache.TryGet(cacheKey, out obj))
				return null;

			return obj as ITable;
		}

		public static void CacheTable(this IQuery query, string cacheKey, ITable table) {
			var tableCache = query.TableCache();
			if (tableCache == null)
				return;

			tableCache.Set(cacheKey, table);
		}

		public static void ClearCachedTables(this IQuery query) {
			var tableCache = query.TableCache();
			if (tableCache == null)
				return;

			tableCache.Clear();
		}

		#region Constraints

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, string[] columnNames) {
			AddPrimaryKey(query, tableName, columnNames, null);
		}

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, string[] columnNames, string constraintName) {
			if (!query.UserCanAlterTable(tableName))
				throw new MissingPrivilegesException(query.UserName(), tableName, Privileges.Alter);

			query.Session.AddPrimaryKey(tableName, columnNames, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, string columnName) {
			AddPrimaryKey(query, tableName, columnName, null);
		}

		public static void AddPrimaryKey(this IQuery query, ObjectName tableName, string columnName, string constraintName) {
			query.AddPrimaryKey(tableName, new[] { columnName }, constraintName);
		}

		public static void AddForeignKey(this IQuery query, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule,
			String constraintName) {
			AddForeignKey(query, table, columns, refTable, refColumns, deleteRule, updateRule, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this IQuery query, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred,
			String constraintName) {
			if (!query.UserCanAlterTable(table))
				throw new MissingPrivilegesException(query.UserName(), table, Privileges.Alter);
			if (!query.UserCanReferenceTable(refTable))
				throw new MissingPrivilegesException(query.UserName(), refTable, Privileges.References);

			query.Session.AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, string[] columns) {
			AddUniqueKey(query, tableName, columns, null);
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, string[] columns, string constraintName) {
			AddUniqueKey(query, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability) {
			AddUniqueKey(query, tableName, columns, deferrability, null);
		}

		public static void AddUniqueKey(this IQuery query, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability, string constraintName) {
			if (!query.UserCanAlterTable(tableName))
				throw new MissingPrivilegesException(query.UserName(), tableName, Privileges.Alter);

			query.Session.AddUniqueKey(tableName, columns, deferrability, constraintName);
		}

		public static void AddCheck(this IQuery query, ObjectName tableName, SqlExpression expression, string constraintName) {
			AddCheck(query, tableName, expression, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddCheck(this IQuery query, ObjectName tableName, SqlExpression expression, ConstraintDeferrability deferred, string constraintName) {
			query.Session.AddCheck(tableName, expression, deferred, constraintName);
		}

		public static void DropAllTableConstraints(this IQuery query, ObjectName tableName) {
			query.Session.DropAllTableConstraints(tableName);
		}

		public static int DropConstraint(this IQuery query, ObjectName tableName, string constraintName) {
			return query.Session.DropTableConstraint(tableName, constraintName);
		}

		public static void AddConstraint(this IQuery query, ObjectName tableName, ConstraintInfo constraintInfo) {
			if (constraintInfo.ConstraintType == ConstraintType.PrimaryKey) {
				var columnNames = constraintInfo.ColumnNames;
				if (columnNames.Length > 1)
					throw new ArgumentException();

				query.AddPrimaryKey(tableName, columnNames[0], constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Unique) {
				query.AddUniqueKey(tableName, constraintInfo.ColumnNames, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Check) {
				query.AddCheck(tableName, constraintInfo.CheckExpression, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.ForeignKey) {
				query.AddForeignKey(tableName, constraintInfo.ColumnNames, constraintInfo.ForeignTable,
					constraintInfo.ForeignColumnNames, constraintInfo.OnDelete, constraintInfo.OnUpdate, constraintInfo.ConstraintName);
			}
		}

		public static bool DropPrimaryKey(this IQuery query, ObjectName tableName, string constraintName) {
			return query.Session.DropTablePrimaryKey(tableName, constraintName);
		}

		public static void CheckConstraints(this IQuery query, ObjectName tableName) {
			query.Session.CheckConstraintViolations(tableName);
		}

		public static ConstraintInfo[] GetTableCheckExpressions(this IQuery query, ObjectName tableName) {
			return query.Session.QueryTableCheckExpressions(tableName);
		}

		public static ConstraintInfo[] GetTableImportedForeignKeys(this IQuery query, ObjectName tableName) {
			return query.Session.QueryTableImportedForeignKeys(tableName);
		}

		public static ConstraintInfo[] GetTableForeignKeys(this IQuery query, ObjectName tableName) {
			return query.Session.QueryTableForeignKeys(tableName);
		}

		public static ConstraintInfo GetTablePrimaryKey(this IQuery query, ObjectName tableName) {
			return query.Session.QueryTablePrimaryKey(tableName);
		}

		public static ConstraintInfo[] GetTableUniqueKeys(this IQuery query, ObjectName tableName) {
			return query.Session.QueryTableUniqueKeys(tableName);
		}

		#endregion

	}
}
