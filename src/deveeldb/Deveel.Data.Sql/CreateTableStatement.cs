// 
//  Copyright 2010  Deveel
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

using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	///<summary>
	/// A parsed state container for the <c>CREATE</c> statement.
	///</summary>
	[Serializable]
	public class CreateTableStatement : Statement {
		/// <summary>
		/// Set to true if this create statement is for a temporary table.
		/// </summary>
		private bool temporary;

		/// <summary>
		/// Only create if table doesn't exist.
		/// </summary>
		private bool onlyIfNotExists;

		/// <summary>
		/// The name of the table to create.
		/// </summary>
		internal string tableNameString;

		/// <summary>
		/// List of column declarations (DataTableColumnInfo)
		/// </summary>
		private IList<DataTableColumnInfo> columns;

		/// <summary>
		/// List of table constraints (SqlConstraint)
		/// </summary>
		private IList constraints;

		/// <summary>
		/// The TableName object.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// Adds a new <see cref="SqlConstraint"/> object to this create statement.
		/// </summary>
		/// <param name="constraint"></param>
		/// <remarks>
		/// A <see cref="SqlConstraint"/> object describes any constraints for the 
		/// new table we are creating.
		/// </remarks>
		internal void AddConstraint(SqlConstraint constraint) {
			constraints.Add(constraint);
		}

		/// <summary>
		/// Creates a DataTableInfo that describes the table that was 
		/// defined by this create statement.
		/// </summary>
		/// <remarks>
		/// This is used by the <see cref="AlterTableStatement">alter statement</see>.
		/// </remarks>
		/// <returns></returns>
		internal DataTableInfo CreateTableInfo() {
			// Make all this information into a DataTableInfo object...
			DataTableInfo tableInfo = new DataTableInfo(tname);
			tableInfo.TableType = "Deveel.Data.VariableSizeDataTableFile";

			// Add the columns.
			// NOTE: Any duplicate column names will be found here...
			foreach (DataTableColumnInfo column in columns) {
				tableInfo.AddColumn(column);
			}

			return tableInfo;
		}


		/// <summary>
		/// Adds a schema constraint to the rules for the schema represented 
		/// by the manager.
		/// </summary>
		/// <param name="manager"></param>
		/// <param name="table"></param>
		/// <param name="constraint"></param>
		internal static void AddSchemaConstraint(DatabaseConnection manager, TableName table, SqlConstraint constraint) {
			if (constraint.Type == ConstraintType.PrimaryKey) {
				string[] columns = new string[constraint.ColumnList.Count];
				constraint.ColumnList.CopyTo(columns, 0);
				manager.AddPrimaryKeyConstraint(table, columns, constraint.Deferrability, constraint.Name);
			} else if (constraint.Type == ConstraintType.ForeignKey) {
				// Currently we forbid referencing a table in another schema
				TableName refTable = Data.TableName.Resolve(constraint.ReferenceTable);
				ConstraintAction updateRule = constraint.UpdateRule;
				ConstraintAction deleteRule = constraint.DeleteRule;
				if (!table.Schema.Equals(refTable.Schema))
					throw new DatabaseException("Foreign key reference error: " +
					                            "Not permitted to reference a table outside of the schema: " +
					                            table + " -> " + refTable);
				string[] columns = new string[constraint.ColumnList.Count];
				constraint.ColumnList.CopyTo(columns, 0);
				string[] refColumns = new string[constraint.ColumnList.Count];
				constraint.ColumnList2.CopyTo(refColumns, 0);
				manager.AddForeignKeyConstraint(
					table, columns,
					refTable, refColumns,
					deleteRule, updateRule, constraint.Deferrability, constraint.Name);
			} else if (constraint.Type == ConstraintType.Unique) {
				string[] columns = new string[constraint.ColumnList.Count];
				constraint.ColumnList.CopyTo(columns, 0);
				manager.AddUniqueConstraint(table, columns, constraint.Deferrability, constraint.Name);
			} else if (constraint.Type == ConstraintType.Check) {
				manager.AddCheckConstraint(table, constraint.original_check_expression,
										   constraint.Deferrability, constraint.Name);
			} else {
				throw new DatabaseException("Unrecognized constraint type.");
			}
		}

		/// <summary>
		/// Returns a <see cref="SqlColumn"/> object a a <see cref="DataTableColumnInfo"/> object.
		/// </summary>
		/// <param name="sqlColumn"></param>
		/// <returns></returns>
		internal static DataTableColumnInfo ConvertToColumnInfo(SqlColumn sqlColumn) {
			TType type = sqlColumn.Type;

			DataTableColumnInfo columnInfo = new DataTableColumnInfo(null, sqlColumn.Name, type);
			columnInfo.IsNotNull = sqlColumn.IsNotNull;

			if (sqlColumn.IndexScheme != null)
				columnInfo.IndexScheme = sqlColumn.IndexScheme;
			if (sqlColumn.Default != null)
				columnInfo.SetDefaultExpression(sqlColumn.original_default_expression);

			return columnInfo;
		}

		/// <summary>
		/// Sets up all constraints specified in this create statement.
		/// </summary>
		internal void SetupAllConstraints() {
			foreach (SqlConstraint constraint in constraints) {
				// Add this to the schema manager tables
				AddSchemaConstraint(Connection, tname, constraint);
			}
		}




		// ---------- Implemented from Statement ----------

		protected override void Prepare() {
			// Get the state from the model
			temporary = GetBoolean("temporary");
			onlyIfNotExists = GetBoolean("only_if_not_exists");
			tableNameString = GetString("table_name");
			IList columnList = GetList("column_list");
			constraints = GetList("constraint_list");

			// Convert column_list to list of com.mckoi.Connection.DataTableColumnInfo
			int size = columnList.Count;
			int identityIndex = -1;
			columns = new List<DataTableColumnInfo>(size);
			for (int i = 0; i < size; ++i) {
				SqlColumn cdef = (SqlColumn)columnList[i];
				if (cdef.Type.SQLType == SqlType.Identity) {
					if (identityIndex != -1)
						throw new DatabaseException("Cannot specify more than one IDENTITY column in a table.");
					identityIndex = i;
				}

				columns.Add(ConvertToColumnInfo(cdef));
			}

			// ----

			string schemaName = Connection.CurrentSchema;
			tname = Data.TableName.Resolve(schemaName, tableNameString);

			string nameStrip = tname.Name;

			if (nameStrip.IndexOf('.') != -1)
				throw new DatabaseException("Table name can not contain '.' character.");

			bool ignoresCase = Connection.IsInCaseInsensitiveMode;

			// Implement the checker class for this statement.
			ColumnChecker checker = new ColumnCheckerImpl(ignoresCase, columns);

			List<string> uniqueColumnList = new List<string>();
			List<string> primaryKeyColumnList = new List<string>();

			// Check the expressions that represent the default values for the columns.
			// Also check each column name
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnInfo columnInfo = columns[i];
				SqlColumn sqlColumn = (SqlColumn)columnList[i];
				checker.CheckExpression(columnInfo.GetDefaultExpression(Connection.System));
				string columnName = columnInfo.Name;

				// If column name starts with [table_name]. then strip it off
				columnInfo.Name = checker.StripTableName(nameStrip, columnName);

				// If unique then add to unique columns
				if (sqlColumn.IsUnique)
					uniqueColumnList.Add(columnName);

				// If primary key then add to primary key columns
				if (sqlColumn.IsPrimaryKey ||
					sqlColumn.Type.SQLType == SqlType.Identity) {
					primaryKeyColumnList.Add(columnName);
				}

				// if identity then set it the default expression
				if (sqlColumn.Type.SQLType == SqlType.Identity) {
					// TableName seq_name = new TableName(tname.Schema, tname.Name + "_IDENTITY");
					columnInfo.SetDefaultExpression(Expression.Parse("UNIQUEKEY('" + tname + "')"));
				}
			}

			// Add the unique and primary key constraints.
			if (uniqueColumnList.Count > 0)
				AddConstraint(SqlConstraint.Unique(uniqueColumnList.ToArray()));
			if (primaryKeyColumnList.Count > 0)
				AddConstraint(SqlConstraint.PrimaryKey(primaryKeyColumnList.ToArray()));

			// Strip the column names and set the expression in all the constraints.
			foreach (SqlConstraint constraint in constraints) {
				checker.StripColumnList(nameStrip, constraint.ColumnList);

				// Check the referencing table for foreign keys
				if (constraint.Type == ConstraintType.ForeignKey) {
					checker.StripColumnList(constraint.ReferenceTable, constraint.column_list2);

					TableName refTname = ResolveTableName(constraint.ReferenceTable);
					if (Connection.IsInCaseInsensitiveMode)
						refTname = Connection.TryResolveCase(refTname);

					constraint.ReferenceTable = refTname.ToString();

					DataTableInfo refTableInfo;
					if (Connection.TableExists(refTname)) {
						// Get the DataTableInfo for the table we are referencing
						refTableInfo = Connection.GetTableInfo(refTname);
					} else if (refTname.Equals(tname)) {
						// We are referencing the table we are creating
						refTableInfo = CreateTableInfo();
					} else {
						throw new DatabaseException(
							  "Referenced table '" + refTname + "' in constraint '" +
							  constraint.Name + "' does not exist.");
					}

					// Resolve columns against the given table info
					refTableInfo.ResolveColumnsInArray(Connection, constraint.column_list2);
				}
				checker.CheckExpression(constraint.CheckExpression);
				checker.CheckColumnList(constraint.ColumnList);
			}
		}

		private class ColumnCheckerImpl : ColumnChecker {
			private readonly bool ignoresCase;
			private readonly IList<DataTableColumnInfo> columns;

			public ColumnCheckerImpl(bool ignoresCase, IList<DataTableColumnInfo> columns) {
				this.ignoresCase = ignoresCase;
				this.columns = columns;
			}

			public override string ResolveColumnName(string columnName) {
				// We need to do case sensitive and case insensitive resolution,
				string foundColumn = null;
				foreach (DataTableColumnInfo column in columns) {
					if (String.Compare(column.Name, columnName, ignoresCase) == 0) {
						if (foundColumn != null)
							throw new DatabaseException("Ambiguous column name '" + columnName + "'");
						foundColumn = column.Name;
					}
				}
				return foundColumn;
			}
		}

		protected override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the schema exist?
			SchemaDef schema = ResolveSchemaName(tname.Schema);
			if (schema == null)
				throw new DatabaseException("Schema '" + tname.Schema + "' doesn't exist.");

			tname = new TableName(schema.Name, tname.Name);

			// Does the user have privs to create this tables?
			if (!Connection.Database.CanUserCreateTableObject(context, User, tname))
				throw new UserAccessException("User not permitted to create table: " + tableNameString);

			// Does the table already exist?
			if (Connection.TableExists(tname)) {
				if (!onlyIfNotExists)
					throw new DatabaseException("Table '" + tname + "' already exists.");

				// Return '0' (0 rows affected).  This happens when we don't create a
				// table (because it exists) and the 'IF NOT EXISTS' clause is present.
				return FunctionTable.ResultTable(context, 0);
			}

			// Report error unless 'if not exists' command is in the statement.
			// Create the data table definition and tell the database to create it.
			DataTableInfo tableInfo = CreateTableInfo();

			if (temporary) {
				Connection.CreateTemporaryTable(tableInfo);
			} else {
				Connection.CreateTable(tableInfo);
			}

			// The initial grants for a table is to give the user who created it
			// full access.
			Connection.GrantManager.Grant(
				Privileges.TableAll, GrantObject.Table, tname.ToString(),
				User.UserName, true, Database.InternalSecureUsername);

			// Set the constraints in the schema.
			SetupAllConstraints();

			// Return '0' if we created the table.  (0 rows affected)
			return FunctionTable.ResultTable(context, 0);
		}
	}
}