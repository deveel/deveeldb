// 
//  AlterTableStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Logic for the <c>ALTER TABLE</c> SQL statement.
	/// </summary>
	public class AlterTableStatement : Statement {
		/// <summary>
		/// The create statement that we use to alter the current table.
		/// </summary>
		/// <remarks>
		/// This is only for compatibility reasons.
		/// </remarks>
		private StatementTree create_statement;

		/// <summary>
		/// The name of the table we are altering.
		/// </summary>
		private String table_name;

		/// <summary>
		/// The list of actions to perform in this alter statement.
		/// </summary>
		private ArrayList actions;

		/// <summary>
		/// The TableName object.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// The prepared create table statement.
		/// </summary>
		private CreateTableStatement create_stmt;



		/// <summary>
		/// Adds an action to perform in this alter statement.
		/// </summary>
		/// <param name="action"></param>
		public void AddAction(AlterTableAction action) {
			if (actions == null)
				actions = new ArrayList();
			actions.Add(action);
		}

		/// <summary>
		/// Returns true if the column names match.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// If the database is in case insensitive mode then the columns 
		/// will match if the case insensitive search matches.
		/// </remarks>
		/// <returns>
		/// </returns>
		public bool CheckColumnNamesMatch(DatabaseConnection db, String col1, String col2) {
			if (db.IsInCaseInsensitiveMode) {
				return String.Compare(col1, col2, true) == 0;
			}
			return col1.Equals(col2);
		}

		private static void CheckColumnConstraint(String col_name, String[] cols, TableName table, String constraint_name) {
			for (int i = 0; i < cols.Length; ++i) {
				if (col_name.Equals(cols[i])) {
					throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.DropColumnViolation,
							  "Constraint violation (" + constraint_name +
							  ") dropping column " + col_name + " because of " +
							  "referential constraint in " + table);
				}
			}

		}



		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		public override void Prepare() {

			// Get variables from the model
			table_name = (String)cmd.GetObject("table_name");
			AddAction((AlterTableAction)cmd.GetObject("alter_action"));
			create_statement = (StatementTree)cmd.GetObject("create_statement");

			// ---

			if (create_statement != null) {
				create_stmt = new CreateTableStatement();
				create_stmt.Init(database, create_statement, null);
				create_stmt.Prepare();
				table_name = create_stmt.table_name;
				//      create_statement.repare(db, user);
			} else {
				// If we don't have a create statement, then this is an SQL alter
				// command.
			}

			//    tname = TableName.Resolve(db.CurrentSchema, table_name);
			tname = ResolveTableName(table_name, database);
			if (tname.Name.IndexOf('.') != -1) {
				throw new DatabaseException("Table name can not contain '.' character.");
			}

		}

		/// <inheritdoc/>
		public override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(database);

			String schema_name = database.CurrentSchema;

			// Does the user have privs to alter this tables?
			if (!database.Database.CanUserAlterTableObject(context, user, tname)) {
				throw new UserAccessException(
				   "User not permitted to alter table: " + table_name);
			}

			if (create_statement != null) {
				// Create the data table definition and tell the database to update it.
				DataTableDef table_def = create_stmt.CreateDataTableDef();
				TableName tname1 = table_def.TableName;
				// Is the table in the database already?
				if (database.TableExists(tname1)) {
					// Drop any schema for this table,
					database.DropAllConstraintsForTable(tname1);
					database.UpdateTable(table_def);
				}
					// If the table isn't in the database,
				else {
					database.CreateTable(table_def);
				}

				// Setup the constraints
				create_stmt.SetupAllConstraints();

				// Return '0' if we created the table.
				return FunctionTable.ResultTable(context, 0);
			} else {
				// SQL alter command using the alter table actions,

				// Get the table definition for the table name,
				DataTableDef table_def = database.GetTable(tname).DataTableDef;
				String table_name = table_def.Name;
				DataTableDef new_table = table_def.NoColumnCopy();

				// Returns a ColumnChecker implementation for this table.
				ColumnChecker checker =
					ColumnChecker.GetStandardColumnChecker(database, tname);

				// Set to true if the table topology is alter, or false if only
				// the constraints are changed.
				bool table_altered = false;

				for (int n = 0; n < table_def.ColumnCount; ++n) {
					DataTableColumnDef column =
						new DataTableColumnDef(table_def[n]);
					String col_name = column.Name;
					// Apply any actions to this column
					bool mark_dropped = false;
					for (int i = 0; i < actions.Count; ++i) {
						AlterTableAction action = (AlterTableAction) actions[i];
						if (action.Action.Equals("ALTERSET") &&
						    CheckColumnNamesMatch(database,
						                          (String) action.Elements[0],
						                          col_name)) {
							Expression exp = (Expression) action.Elements[1];
							checker.CheckExpression(exp);
							column.SetDefaultExpression(exp);
							table_altered = true;
						} else if (action.Action.Equals("DROPDEFAULT") &&
						           CheckColumnNamesMatch(database,
						                                 (String) action.Elements[0],
						                                 col_name)) {
							column.SetDefaultExpression(null);
							table_altered = true;
						} else if (action.Action.Equals("DROP") &&
						           CheckColumnNamesMatch(database,
						                                 (String) action.Elements[0],
						                                 col_name)) {
							// Check there are no referential links to this column
							Transaction.ColumnGroupReference[] refs =
								database.QueryTableImportedForeignKeyReferences(tname);
							for (int p = 0; p < refs.Length; ++p) {
								CheckColumnConstraint(col_name, refs[p].ref_columns,
								                      refs[p].ref_table_name, refs[p].name);
							}
							// Or from it
							refs = database.QueryTableForeignKeyReferences(tname);
							for (int p = 0; p < refs.Length; ++p) {
								CheckColumnConstraint(col_name, refs[p].key_columns,
								                      refs[p].key_table_name, refs[p].name);
							}
							// Or that it's part of a primary key
							Transaction.ColumnGroup primary_key =
								database.QueryTablePrimaryKeyGroup(tname);
							if (primary_key != null) {
								CheckColumnConstraint(col_name, primary_key.columns,
								                      tname, primary_key.name);
							}
							// Or that it's part of a unique set
							Transaction.ColumnGroup[] uniques =
								database.QueryTableUniqueGroups(tname);
							for (int p = 0; p < uniques.Length; ++p) {
								CheckColumnConstraint(col_name, uniques[p].columns,
								                      tname, uniques[p].name);
							}

							mark_dropped = true;
							table_altered = true;
						}
					}
					// If not dropped then add to the new table definition.
					if (!mark_dropped) {
						new_table.AddColumn(column);
					}
				}

				// Add any new columns,
				for (int i = 0; i < actions.Count; ++i) {
					AlterTableAction action = (AlterTableAction) actions[i];
					if (action.Action.Equals("ADD")) {
						ColumnDef cdef = (ColumnDef) action.Elements[0];
						if (cdef.IsUnique || cdef.IsPrimaryKey) {
							throw new DatabaseException("Can not use UNIQUE or PRIMARY KEY " +
							                            "column constraint when altering a column.  Use " +
							                            "ADD CONSTRAINT instead.");
						}
						// Convert to a DataTableColumnDef
						DataTableColumnDef col = CreateTableStatement.ConvertColumnDef(cdef);

						checker.CheckExpression(
							col.GetDefaultExpression(database.System));
						String col_name = col.Name;
						// If column name starts with [table_name]. then strip it off
						col.Name = checker.StripTableName(table_name, col_name);
						new_table.AddColumn(col);
						table_altered = true;
					}
				}

				// Any constraints to drop...
				for (int i = 0; i < actions.Count; ++i) {
					AlterTableAction action = (AlterTableAction) actions[i];
					if (action.Action.Equals("DROP_CONSTRAINT")) {
						String constraint_name = (String) action.Elements[0];
						int drop_count = database.DropNamedConstraint(tname, constraint_name);
						if (drop_count == 0) {
							throw new DatabaseException(
								"Named constraint to drop on table " + tname +
								" was not found: " + constraint_name);
						}
					} else if (action.Action.Equals("DROP_CONSTRAINT_PRIMARY_KEY")) {
						bool constraint_dropped =
							database.DropPrimaryKeyConstraintForTable(tname, null);
						if (!constraint_dropped) {
							throw new DatabaseException(
								"No primary key to delete on table " + tname);
						}
					}
				}

				// Any constraints to add...
				for (int i = 0; i < actions.Count; ++i) {
					AlterTableAction action = (AlterTableAction) actions[i];
					if (action.Action.Equals("ADD_CONSTRAINT")) {
						ConstraintDef constraint = (ConstraintDef) action.Elements[0];
						bool foreign_constraint =
							(constraint.type == ConstraintType.ForeignKey);
						TableName ref_tname = null;
						if (foreign_constraint) {
							ref_tname =
								ResolveTableName(constraint.reference_table_name, database);
							if (database.IsInCaseInsensitiveMode) {
								ref_tname = database.TryResolveCase(ref_tname);
							}
							constraint.reference_table_name = ref_tname.ToString();
						}

						checker.StripColumnList(table_name, constraint.column_list);
						checker.StripColumnList(constraint.reference_table_name,
						                        constraint.column_list2);
						checker.CheckExpression(constraint.check_expression);
						checker.CheckColumnList(constraint.column_list);
						if (foreign_constraint && constraint.column_list2 != null) {
							ColumnChecker referenced_checker =
								ColumnChecker.GetStandardColumnChecker(database, ref_tname);
							referenced_checker.CheckColumnList(constraint.column_list2);
						}

						CreateTableStatement.AddSchemaConstraint(database, tname, constraint);

					}
				}

				// Alter the existing table to the new format...
				if (table_altered) {
					if (new_table.ColumnCount == 0) {
						throw new DatabaseException(
							"Can not ALTER table to have 0 columns.");
					}
					database.UpdateTable(new_table);
				} else {
					// If the table wasn't physically altered, check the constraints.
					// Calling this method will also make the transaction check all
					// deferred constraints during the next commit.
					database.CheckAllConstraints(tname);
				}

				// Return '0' if everything successful.
				return FunctionTable.ResultTable(context, 0);
			}
		}
	}
}