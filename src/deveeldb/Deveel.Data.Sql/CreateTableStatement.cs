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

namespace Deveel.Data.Sql {
	///<summary>
	/// A parsed state container for the <c>CREATE</c> statement.
	///</summary>
	public class CreateTableStatement : Statement {
		/// <summary>
		/// Set to true if this create statement is for a temporary table.
		/// </summary>
		private bool temporary = false;

		/// <summary>
		/// Only create if table doesn't exist.
		/// </summary>
		private bool only_if_not_exists = false;

		/// <summary>
		/// The name of the table to create.
		/// </summary>
		internal String table_name;

		/// <summary>
		/// List of column declarations (SqlColumn)
		/// </summary>
		private IList columns;

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
		internal void AddConstraintDef(SqlConstraint constraint) {
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
		internal DataTableInfo CreateDataTableDef() {
			// Make all this information into a DataTableInfo object...
			DataTableInfo tableInfo = new DataTableInfo();
			tableInfo.TableName = tname;

			// Add the columns.
			// NOTE: Any duplicate column names will be found here...
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnInfo cd = (DataTableColumnInfo)columns[i];
				tableInfo.AddColumn(cd);
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
		internal static void AddSchemaConstraint(DatabaseConnection manager,
										TableName table, SqlConstraint constraint) {
			if (constraint.Type == ConstraintType.PrimaryKey) {
				manager.AddPrimaryKeyConstraint(table,
					constraint.ColumnList, constraint.Deferrability, constraint.Name);
			} else if (constraint.Type == ConstraintType.ForeignKey) {
				// Currently we forbid referencing a table in another schema
				TableName ref_table =
									TableName.Resolve(constraint.ReferenceTable);
				ConstraintAction update_rule = constraint.UpdateRule;
				ConstraintAction delete_rule = constraint.DeleteRule;
				if (table.Schema.Equals(ref_table.Schema)) {
					manager.AddForeignKeyConstraint(
						 table, constraint.ColumnList,
						 ref_table, constraint.ColumnList2,
						 delete_rule, update_rule, constraint.Deferrability, constraint.Name);
				} else {
					throw new DatabaseException("Foreign key reference error: " +
							"Not permitted to reference a table outside of the schema: " +
							table + " -> " + ref_table);
				}
			} else if (constraint.Type == ConstraintType.Unique) {
				manager.AddUniqueConstraint(table, constraint.ColumnList,
											constraint.Deferrability, constraint.Name);
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
		/// <param name="cdef"></param>
		/// <returns></returns>
		internal static DataTableColumnInfo ConvertColumnDef(SqlColumn cdef) {
			TType type = cdef.Type;

			DataTableColumnInfo dtcdef = new DataTableColumnInfo();
			dtcdef.Name = cdef.Name;
			dtcdef.IsNotNull = cdef.IsNotNull;
			dtcdef.SetFromTType(type);

			if (cdef.IndexScheme != null) {
				dtcdef.IndexScheme = cdef.IndexScheme;
			}
			if (cdef.Default != null) {
				dtcdef.SetDefaultExpression(cdef.original_default_expression);
			}

			dtcdef.InitTTypeInfo();
			return dtcdef;
		}

		/// <summary>
		/// Sets up all constraints specified in this create statement.
		/// </summary>
		internal void SetupAllConstraints() {
			for (int i = 0; i < constraints.Count; ++i) {
				SqlConstraint constraint = (SqlConstraint)constraints[i];

				// Add this to the schema manager tables
				AddSchemaConstraint(Connection, tname, constraint);
			}
		}




		// ---------- Implemented from Statement ----------

		protected override void Prepare() {

			// Get the state from the model
			temporary = GetBoolean("temporary");
			only_if_not_exists = GetBoolean("only_if_not_exists");
			table_name = GetString("table_name");
			IList column_list = GetList("column_list");
			constraints = GetList("constraint_list");

			// Convert column_list to list of com.mckoi.Connection.DataTableColumnInfo
			int size = column_list.Count;
			int identityIndex = -1;
			columns = new ArrayList(size);
			for (int i = 0; i < size; ++i) {
				SqlColumn cdef = (SqlColumn)column_list[i];
				if (cdef.Type.SQLType == SqlType.Identity) {
					if (identityIndex != -1)
						throw new DatabaseException("Cannot specify more than one IDENTITY column in a table.");
					identityIndex = i;
				}
				columns.Add(ConvertColumnDef(cdef));
			}

			// ----

			String schema_name = Connection.CurrentSchema;
			tname = TableName.Resolve(schema_name, table_name);

			String name_strip = tname.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Table name can not contain '.' character.");

			bool ignores_case = Connection.IsInCaseInsensitiveMode;

			// Implement the checker class for this statement.
			ColumnChecker checker = new ColumnCheckerImpl(ignores_case, columns);

			ArrayList unique_column_list = new ArrayList();
			ArrayList primary_key_column_list = new ArrayList();

			// Check the expressions that represent the default values for the columns.
			// Also check each column name
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnInfo cdef = (DataTableColumnInfo)columns[i];
				SqlColumn model_cdef = (SqlColumn)column_list[i];
				checker.CheckExpression(cdef.GetDefaultExpression(Connection.System));
				String col_name = cdef.Name;
				// If column name starts with [table_name]. then strip it off
				cdef.Name = checker.StripTableName(name_strip, col_name);
				// If unique then add to unique columns
				if (model_cdef.IsUnique) {
					unique_column_list.Add(col_name);
				}
				// If primary key then add to primary key columns
				if (model_cdef.IsPrimaryKey ||
					model_cdef.Type.SQLType == SqlType.Identity) {
					primary_key_column_list.Add(col_name);
				}
				// if identity then set it the default expression
				if (model_cdef.Type.SQLType == SqlType.Identity) {
					// TableName seq_name = new TableName(tname.Schema, tname.Name + "_IDENTITY");
					cdef.SetDefaultExpression(Expression.Parse("UNIQUEKEY('" + tname + "')"));
				}
			}

			// Add the unique and primary key constraints.
			if (unique_column_list.Count > 0) {
				AddConstraintDef(SqlConstraint.Unique((string[])unique_column_list.ToArray(typeof(string))));
			}
			if (primary_key_column_list.Count > 0) {
				AddConstraintDef(SqlConstraint.PrimaryKey((string[])primary_key_column_list.ToArray(typeof(string))));
			}

			// Strip the column names and set the expression in all the constraints.
			for (int i = 0; i < constraints.Count; ++i) {
				SqlConstraint constraint = (SqlConstraint)constraints[i];
				checker.StripColumnList(name_strip, constraint.column_list);
				// Check the referencing table for foreign keys
				if (constraint.Type == ConstraintType.ForeignKey) {
					checker.StripColumnList(constraint.ReferenceTable,
											constraint.column_list2);
					TableName ref_tname =
							 ResolveTableName(constraint.ReferenceTable, Connection);
					if (Connection.IsInCaseInsensitiveMode) {
						ref_tname = Connection.TryResolveCase(ref_tname);
					}
					constraint.ReferenceTable = ref_tname.ToString();

					DataTableInfo refTableInfo;
					if (Connection.TableExists(ref_tname)) {
						// Get the DataTableInfo for the table we are referencing
						refTableInfo = Connection.GetDataTableDef(ref_tname);
					} else if (ref_tname.Equals(tname)) {
						// We are referencing the table we are creating
						refTableInfo = CreateDataTableDef();
					} else {
						throw new DatabaseException(
							  "Referenced table '" + ref_tname + "' in constraint '" +
							  constraint.Name + "' does not exist.");
					}
					// Resolve columns against the given table info
					refTableInfo.ResolveColumnsInList(Connection, constraint.column_list2);

				}
				checker.CheckExpression(constraint.CheckExpression);
				checker.CheckColumnList(constraint.column_list);
			}
		}

		private class ColumnCheckerImpl : ColumnChecker {
			private bool ignores_case;
			private IList columns;

			public ColumnCheckerImpl(bool ignoresCase, IList columns) {
				ignores_case = ignoresCase;
				this.columns = columns;
			}

			internal override String ResolveColumnName(String col_name) {
				// We need to do case sensitive and case insensitive resolution,
				String found_col = null;
				for (int n = 0; n < columns.Count; ++n) {
					DataTableColumnInfo col = (DataTableColumnInfo)columns[n];
					if (!ignores_case) {
						if (col.Name.Equals(col_name)) {
							return col_name;
						}
					} else {
						if (String.Compare(col.Name, col_name, true) == 0) {
							if (found_col != null) {
								throw new DatabaseException("Ambiguous column name '" +
															col_name + "'");
							}
							found_col = col.Name;
						}
					}
				}
				return found_col;
			}
		}

		protected override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the schema exist?
			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema =
					Connection.ResolveSchemaCase(tname.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + tname.Schema +
											"' doesn't exist.");
			} else {
				tname = new TableName(schema.Name, tname.Name);
			}

			// Does the user have privs to create this tables?
			if (!Connection.Database.CanUserCreateTableObject(context, User, tname))
				throw new UserAccessException("User not permitted to create table: " + table_name);


			// PENDING: Creation of temporary tables...


			// Does the table already exist?
			if (!Connection.TableExists(tname)) {

				// Create the data table definition and tell the database to create
				// it.
				DataTableInfo tableInfo = CreateDataTableDef();
				Connection.CreateTable(tableInfo);

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

			// Report error unless 'if not exists' command is in the statement.
			if (only_if_not_exists == false) {
				throw new DatabaseException("Table '" + tname + "' already exists.");
			}

			// Return '0' (0 rows affected).  This happens when we don't create a
			// table (because it exists) and the 'IF NOT EXISTS' clause is present.
			return FunctionTable.ResultTable(context, 0);

		}

	}
}