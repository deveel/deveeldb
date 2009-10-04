//  
//  CreateTableStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		/// List of column declarations (ColumnDef)
		/// </summary>
		private ArrayList columns;

		/// <summary>
		/// List of table constraints (ConstraintDef)
		/// </summary>
		private ArrayList constraints;

		/// <summary>
		/// The TableName object.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// Adds a new <see cref="ConstraintDef"/> object to this create statement.
		/// </summary>
		/// <param name="constraint"></param>
		/// <remarks>
		/// A <see cref="ConstraintDef"/> object describes any constraints for the 
		/// new table we are creating.
		/// </remarks>
		internal void AddConstraintDef(ConstraintDef constraint) {
			constraints.Add(constraint);
		}

		/// <summary>
		/// Creates a DataTableDef that describes the table that was 
		/// defined by this create statement.
		/// </summary>
		/// <remarks>
		/// This is used by the <see cref="AlterTableStatement">alter statement</see>.
		/// </remarks>
		/// <returns></returns>
		internal DataTableDef CreateDataTableDef() {
			// Make all this information into a DataTableDef object...
			DataTableDef table_def = new DataTableDef();
			table_def.TableName = tname;
			table_def.TableType = "Deveel.Data.VariableSizeDataTableFile";

			// Add the columns.
			// NOTE: Any duplicate column names will be found here...
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnDef cd = (DataTableColumnDef)columns[i];
				table_def.AddColumn(cd);
			}

			return table_def;
		}


		/// <summary>
		/// Adds a schema constraint to the rules for the schema represented 
		/// by the manager.
		/// </summary>
		/// <param name="manager"></param>
		/// <param name="table"></param>
		/// <param name="constraint"></param>
		internal static void AddSchemaConstraint(DatabaseConnection manager,
										TableName table, ConstraintDef constraint) {
			if (constraint.type == ConstraintType.PrimaryKey) {
				manager.AddPrimaryKeyConstraint(table,
					constraint.ColumnList, constraint.deferred, constraint.Name);
			} else if (constraint.type == ConstraintType.ForeignKey) {
				// Currently we forbid referencing a table in another schema
				TableName ref_table =
									TableName.Resolve(constraint.reference_table_name);
				String update_rule = constraint.UpdateRule.ToUpper();
				String delete_rule = constraint.DeleteRule.ToUpper();
				if (table.Schema.Equals(ref_table.Schema)) {
					manager.AddForeignKeyConstraint(
						 table, constraint.ColumnList,
						 ref_table, constraint.ColumnList2,
						 delete_rule, update_rule, constraint.deferred, constraint.Name);
				} else {
					throw new DatabaseException("Foreign key reference error: " +
							"Not permitted to reference a table outside of the schema: " +
							table + " -> " + ref_table);
				}
			} else if (constraint.type == ConstraintType.Unique) {
				manager.AddUniqueConstraint(table, constraint.ColumnList,
											constraint.deferred, constraint.Name);
			} else if (constraint.type == ConstraintType.Check) {
				manager.AddCheckConstraint(table, constraint.original_check_expression,
										   constraint.deferred, constraint.Name);
			} else {
				throw new DatabaseException("Unrecognized constraint type.");
			}
		}

		/// <summary>
		/// Returns a <see cref="ColumnDef"/> object a a <see cref="DataTableColumnDef"/> object.
		/// </summary>
		/// <param name="cdef"></param>
		/// <returns></returns>
		internal static DataTableColumnDef ConvertColumnDef(ColumnDef cdef) {
			TType type = cdef.Type;

			DataTableColumnDef dtcdef = new DataTableColumnDef();
			dtcdef.Name = cdef.Name;
			dtcdef.IsNotNull = cdef.IsNotNull;
			dtcdef.SetFromTType(type);

			if (cdef.IndexScheme != null) {
				dtcdef.IndexScheme = cdef.IndexScheme;
			}
			if (cdef.default_expression != null) {
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
				ConstraintDef constraint = (ConstraintDef)constraints[i];

				// Add this to the schema manager tables
				AddSchemaConstraint(database, tname, constraint);
			}
		}




		// ---------- Implemented from Statement ----------

		public override void Prepare() {

			// Get the state from the model
			temporary = cmd.GetBoolean("temporary");
			only_if_not_exists = cmd.GetBoolean("only_if_not_exists");
			table_name = (String)cmd.GetObject("table_name");
			ArrayList column_list = (ArrayList)cmd.GetObject("column_list");
			constraints = (ArrayList)cmd.GetObject("constraint_list");

			// Convert column_list to list of com.mckoi.database.DataTableColumnDef
			int size = column_list.Count;
			columns = new ArrayList(size);
			for (int i = 0; i < size; ++i) {
				ColumnDef cdef = (ColumnDef)column_list[i];
				columns.Add(ConvertColumnDef(cdef));
			}

			// ----

			String schema_name = database.CurrentSchema;
			tname = TableName.Resolve(schema_name, table_name);

			String name_strip = tname.Name;

			if (name_strip.IndexOf('.') != -1) {
				throw new DatabaseException("Table name can not contain '.' character.");
			}

			bool ignores_case = database.IsInCaseInsensitiveMode;

			// Implement the checker class for this statement.
			ColumnChecker checker = new ColumnCheckerImpl(ignores_case, columns);

			ArrayList unique_column_list = new ArrayList();
			ArrayList primary_key_column_list = new ArrayList();

			// Check the expressions that represent the default values for the columns.
			// Also check each column name
			for (int i = 0; i < columns.Count; ++i) {
				DataTableColumnDef cdef = (DataTableColumnDef)columns[i];
				ColumnDef model_cdef = (ColumnDef)column_list[i];
				checker.CheckExpression(cdef.GetDefaultExpression(database.System));
				String col_name = cdef.Name;
				// If column name starts with [table_name]. then strip it off
				cdef.Name = checker.StripTableName(name_strip, col_name);
				// If unique then add to unique columns
				if (model_cdef.IsUnique) {
					unique_column_list.Add(col_name);
				}
				// If primary key then add to primary key columns
				if (model_cdef.IsPrimaryKey) {
					primary_key_column_list.Add(col_name);
				}
			}

			// Add the unique and primary key constraints.
			if (unique_column_list.Count > 0) {
				ConstraintDef constraint = new ConstraintDef();
				constraint.SetUnique(unique_column_list);
				AddConstraintDef(constraint);
			}
			if (primary_key_column_list.Count > 0) {
				ConstraintDef constraint = new ConstraintDef();
				constraint.SetPrimaryKey(primary_key_column_list);
				AddConstraintDef(constraint);
			}

			// Strip the column names and set the expression in all the constraints.
			for (int i = 0; i < constraints.Count; ++i) {
				ConstraintDef constraint = (ConstraintDef)constraints[i];
				checker.StripColumnList(name_strip, constraint.column_list);
				// Check the referencing table for foreign keys
				if (constraint.type == ConstraintType.ForeignKey) {
					checker.StripColumnList(constraint.reference_table_name,
											constraint.column_list2);
					TableName ref_tname =
							 ResolveTableName(constraint.reference_table_name, database);
					if (database.IsInCaseInsensitiveMode) {
						ref_tname = database.TryResolveCase(ref_tname);
					}
					constraint.reference_table_name = ref_tname.ToString();

					DataTableDef ref_table_def;
					if (database.TableExists(ref_tname)) {
						// Get the DataTableDef for the table we are referencing
						ref_table_def = database.GetDataTableDef(ref_tname);
					} else if (ref_tname.Equals(tname)) {
						// We are referencing the table we are creating
						ref_table_def = CreateDataTableDef();
					} else {
						throw new DatabaseException(
							  "Referenced table '" + ref_tname + "' in constraint '" +
							  constraint.Name + "' does not exist.");
					}
					// Resolve columns against the given table def
					ref_table_def.ResolveColumnsInArray(database, constraint.column_list2);

				}
				checker.CheckExpression(constraint.check_expression);
				checker.CheckColumnList(constraint.column_list);
			}

		}

		private class ColumnCheckerImpl : ColumnChecker {
			private bool ignores_case;
			private ArrayList columns;

			public ColumnCheckerImpl(bool ignoresCase, ArrayList columns) {
				ignores_case = ignoresCase;
				this.columns = columns;
			}

			internal override String ResolveColumnName(String col_name) {
				// We need to do case sensitive and case insensitive resolution,
				String found_col = null;
				for (int n = 0; n < columns.Count; ++n) {
					DataTableColumnDef col = (DataTableColumnDef)columns[n];
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

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Does the schema exist?
			bool ignore_case = database.IsInCaseInsensitiveMode;
			SchemaDef schema =
					database.ResolveSchemaCase(tname.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + tname.Schema +
											"' doesn't exist.");
			} else {
				tname = new TableName(schema.Name, tname.Name);
			}

			// Does the user have privs to create this tables?
			if (!database.Database.CanUserCreateTableObject(context,
																 user, tname)) {
				throw new UserAccessException(
				   "User not permitted to create table: " + table_name);
			}



			// PENDING: Creation of temporary tables...




			// Does the table already exist?
			if (!database.TableExists(tname)) {

				// Create the data table definition and tell the database to create
				// it.
				DataTableDef table_def = CreateDataTableDef();
				database.CreateTable(table_def);

				// The initial grants for a table is to give the user who created it
				// full access.
				database.GrantManager.Grant(
					 Privileges.TableAll, GrantObject.Table, tname.ToString(),
					 user.UserName, true, Database.InternalSecureUsername);

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