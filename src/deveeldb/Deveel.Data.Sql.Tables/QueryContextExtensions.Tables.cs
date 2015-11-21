using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data.Caching;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Sql.Tables {
	public static partial class QueryContextExtensions {
		public static ObjectName ResolveTableName(this IQueryContext context, ObjectName tableName) {
			return context.Session().ResolveTableName(tableName);
		}

		public static ObjectName ResolveTableName(this IQueryContext context, string name) {
			var schema = context.CurrentSchema;
			if (String.IsNullOrEmpty(schema))
				throw new InvalidOperationException("Default schema not specified in the context.");

			var objSchemaName = context.ResolveSchemaName(schema);
			if (objSchemaName == null)
				throw new InvalidOperationException(
					String.Format("The default schema of the session '{0}' is not defined in the database.", schema));

			var objName = ObjectName.Parse(name);
			if (objName.Parent == null)
				objName = new ObjectName(objSchemaName, objName.Name);

			return context.ResolveTableName(objName);
		}

		public static bool TableExists(this IQueryContext context, ObjectName tableName) {
			return context.ObjectExists(DbObjectType.Table, tableName);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo) {
			CreateTable(context, tableInfo, false);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo, bool onlyIfNotExists) {
			CreateTable(context, tableInfo, onlyIfNotExists, false);
		}

		public static void CreateTable(this IQueryContext context, TableInfo tableInfo, bool onlyIfNotExists, bool temporary) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!context.UserCanCreateTable(tableName))
				throw new MissingPrivilegesException(context.User().Name, tableName, Privileges.Create);

			if (context.TableExists(tableName)) {
				if (!onlyIfNotExists)
					throw new InvalidOperationException(
						String.Format("The table {0} already exists and the IF NOT EXISTS clause was not specified.", tableName));

				return;
			}

			context.Session().CreateTable(tableInfo, temporary);

			using (var systemContext = context.ForSystemUser()) {
				systemContext.GrantToUserOnTable(tableInfo.TableName, context.User().Name, Privileges.TableAll);
			}
		}

		internal static void CreateSystemTable(this IQueryContext context, TableInfo tableInfo) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");

			var tableName = tableInfo.TableName;

			if (!context.UserCanCreateTable(tableName))
				throw new MissingPrivilegesException(context.User().Name, tableName, Privileges.Create);

			context.Session().CreateTable(tableInfo, false);
		}

		public static void DropTables(this IQueryContext context, IEnumerable<ObjectName> tableNames) {
			DropTables(context, tableNames, false);
		}

		public static void DropTables(this IQueryContext context, IEnumerable<ObjectName> tableNames, bool onlyIfExists) {
			var tableNameList = tableNames.ToList();
			foreach (var tableName in tableNameList) {
				if (!context.UserCanDropObject(DbObjectType.Table, tableName))
					throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Drop);
			}

			// Check there are no referential links to any tables being dropped
			foreach (var tableName in tableNameList) {
				var refs = context.GetTableImportedForeignKeys(tableName);

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
					if (!context.TableExists(tableName)) {
						throw new InvalidOperationException(String.Format("The table '{0}' does not exist and cannot be dropped.",
							tableName));
					}
				}
			}

			foreach (var tname in tableNameList) {
				// Does the table already exist?
				if (context.TableExists(tname)) {
					// Drop table in the transaction
					context.DropObject(DbObjectType.Table, tname);

					// Revoke all the grants on the table
					context.RevokeAllGrantsOnTable(tname);

					// Drop all constraints from the schema
					context.DropAllTableConstraints(tname);
				}
			}
		}

		public static void DropTable(this IQueryContext context, ObjectName tableName) {
			DropTable(context, tableName, false);
		}

		public static void DropTable(this IQueryContext context, ObjectName tableName, bool onlyIfExists) {
			context.DropTables(new[] {tableName}, onlyIfExists);
		}

		public static void AlterTable(this IQueryContext context, TableInfo tableInfo) {
			context.AlterObject(tableInfo);
		}

		public static ITable GetTable(this IQueryContext context, ObjectName tableName) {
			var table = context.GetCachedTable(tableName.FullName) as ITable;
			if (table == null) {
				table = context.Session().GetTable(tableName);
				if (table != null) {
					table = new UserContextTable(context, table);
					context.CacheTable(tableName.FullName, table);
				}
			}

			return table;
		}

		public static IMutableTable GetMutableTable(this IQueryContext context, ObjectName tableName) {
			return context.GetTable(tableName) as IMutableTable;
		}

		private static ICache TableCache(this IQueryContext context) {
			return context.ResolveService<ICache>("TableCache");
		}

		public static ITable GetCachedTable(this IQueryContext context, string cacheKey) {
			var tableCache = context.TableCache();
			if (tableCache == null)
				return null;

			object obj;
			if (!tableCache.TryGet(cacheKey, out obj))
				return null;

			return obj as ITable;
		}

		public static void CacheTable(this IQueryContext context, string cacheKey, ITable table) {
			var tableCache = context.TableCache();
			if (tableCache == null)
				return;

			tableCache.Set(cacheKey, table);
		}

		public static void ClearCachedTables(this IQueryContext context) {
			var tableCache = context.TableCache();
			if (tableCache == null)
				return;

			tableCache.Clear();
		}

		#region Constraints

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string[] columnNames) {
			AddPrimaryKey(context, tableName, columnNames, null);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string[] columnNames, string constraintName) {
			if (!context.UserCanAlterTable(tableName))
				throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Alter);

			context.Session().AddPrimaryKey(tableName, columnNames, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string columnName) {
			AddPrimaryKey(context, tableName, columnName, null);
		}

		public static void AddPrimaryKey(this IQueryContext context, ObjectName tableName, string columnName, string constraintName) {
			context.AddPrimaryKey(tableName, new[] { columnName }, constraintName);
		}

		public static void AddForeignKey(this IQueryContext context, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule,
			String constraintName) {
			AddForeignKey(context, table, columns, refTable, refColumns, deleteRule, updateRule, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this IQueryContext context, ObjectName table, string[] columns, ObjectName refTable,
			string[] refColumns, ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred,
			String constraintName) {
			if (!context.UserCanAlterTable(table))
				throw new MissingPrivilegesException(context.UserName(), table, Privileges.Alter);
			if (!context.UserCanReferenceTable(refTable))
				throw new MissingPrivilegesException(context.UserName(), refTable, Privileges.References);

			context.Session().AddForeignKey(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns) {
			AddUniqueKey(context, tableName, columns, null);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns, string constraintName) {
			AddUniqueKey(context, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability) {
			AddUniqueKey(context, tableName, columns, deferrability, null);
		}

		public static void AddUniqueKey(this IQueryContext context, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferrability, string constraintName) {
			if (!context.UserCanAlterTable(tableName))
				throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Alter);

			context.Session().AddUniqueKey(tableName, columns, deferrability, constraintName);
		}

		public static void AddCheck(this IQueryContext context, ObjectName tableName, SqlExpression expression, string constraintName) {
			AddCheck(context, tableName, expression, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddCheck(this IQueryContext context, ObjectName tableName, SqlExpression expression, ConstraintDeferrability deferred, string constraintName) {
			context.Session().AddCheck(tableName, expression, deferred, constraintName);
		}

		public static void DropAllTableConstraints(this IQueryContext context, ObjectName tableName) {
			context.Session().DropAllTableConstraints(tableName);
		}

		public static int DropConstraint(this IQueryContext context, ObjectName tableName, string constraintName) {
			return context.Session().DropTableConstraint(tableName, constraintName);
		}

		public static void AddConstraint(this IQueryContext context, ObjectName tableName, ConstraintInfo constraintInfo) {
			if (constraintInfo.ConstraintType == ConstraintType.PrimaryKey) {
				var columnNames = constraintInfo.ColumnNames;
				if (columnNames.Length > 1)
					throw new ArgumentException();

				context.AddPrimaryKey(tableName, columnNames[0], constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Unique) {
				context.AddUniqueKey(tableName, constraintInfo.ColumnNames, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.Check) {
				context.AddCheck(tableName, constraintInfo.CheckExpression, constraintInfo.ConstraintName);
			} else if (constraintInfo.ConstraintType == ConstraintType.ForeignKey) {
				context.AddForeignKey(tableName, constraintInfo.ColumnNames, constraintInfo.ForeignTable,
					constraintInfo.ForeignColumnNames, constraintInfo.OnDelete, constraintInfo.OnUpdate, constraintInfo.ConstraintName);
			}
		}

		public static bool DropPrimaryKey(this IQueryContext context, ObjectName tableName, string constraintName) {
			return context.Session().DropTablePrimaryKey(tableName, constraintName);
		}

		public static void CheckConstraints(this IQueryContext context, ObjectName tableName) {
			context.Session().CheckConstraintViolations(tableName);
		}

		public static ConstraintInfo[] GetTableCheckExpressions(this IQueryContext context, ObjectName tableName) {
			return context.Session().QueryTableCheckExpressions(tableName);
		}

		public static ConstraintInfo[] GetTableImportedForeignKeys(this IQueryContext context, ObjectName tableName) {
			return context.Session().QueryTableImportedForeignKeys(tableName);
		}

		public static ConstraintInfo[] GetTableForeignKeys(this IQueryContext context, ObjectName tableName) {
			return context.Session().QueryTableForeignKeys(tableName);
		}

		public static ConstraintInfo GetTablePrimaryKey(this IQueryContext context, ObjectName tableName) {
			return context.Session().QueryTablePrimaryKey(tableName);
		}

		public static ConstraintInfo[] GetTableUniqueKeys(this IQueryContext context, ObjectName tableName) {
			return context.Session().QueryTableUniqueKeys(tableName);
		}

		#endregion
	}
}