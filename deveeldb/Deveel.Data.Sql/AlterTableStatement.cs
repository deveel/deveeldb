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
		private string table_name;

		/// <summary>
		/// The list of actions to perform in this alter statement.
		/// </summary>
		private IList actions;

		/// <summary>
		/// The TableName object.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// The prepared create table statement.
		/// </summary>
		private CreateTableStatement create_stmt;

		/// <summary>
		/// Gets or sets the name (qualified or unqualified) of 
		/// the table to alter.
		/// </summary>
		public string TableName {
			get { return GetString("table_name"); }
			set {
				if (value == null || value.Length == 0)
					throw new ArgumentNullException("value");
				
				SetValue("table_name", value);
			}
		}

		public CreateTableStatement CreateStatement {
			get { return (CreateTableStatement) GetValue("create_statement"); }
			set {
				if (!IsEmpty("alter_action"))
					throw new StatementException("Cannot set a CREATE statement if other actions were set.");
				if (value == null) {
					SetValue("create_statement", (StatementTree) null);
				} else {
					SetValue("create_statement", value.Info);
				}
			}
		}

		public IList Actions {
			get { return GetList("alter_actions", true); }
		}

		protected override bool OnListAdd(string key, object value, ref object newValue) {
			if (key == "alter_actions") {
				if (!(value is AlterTableAction))
					throw new ArgumentException("The value must be a " + typeof(AlterTableAction) + ".");

				if (ContainsKey("create_statement"))
					throw new StatementException("Cannot add an action if the CREATE statement was already set.");
			}

			return base.OnListAdd(key, value, ref newValue);
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
		protected override void Prepare() {

			// Get variables from the model
			table_name = GetString("table_name");
			actions = GetList("alter_actions", true);
			if (ContainsKey("alter_action")) {
				actions.Add(GetValue("alter_action"));
			}

			create_statement = (StatementTree)GetValue("create_statement");

			// ---

			if (create_statement != null) {
				create_stmt = new CreateTableStatement();
				create_stmt.Init(Connection, create_statement, null);
				create_stmt.PrepareStatement();
				table_name = create_stmt.table_name;
				//      create_statement.Prepare(db, User);
			} else {
				// If we don't have a create statement, then this is an SQL alter
				// command.
			}

			//    tname = TableName.Resolve(db.CurrentSchema, table_name);
			tname = ResolveTableName(table_name, Connection);
			if (tname.Name.IndexOf('.') != -1) {
				throw new DatabaseException("Table name can not contain '.' character.");
			}

		}

		/// <inheritdoc/>
		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String schema_name = Connection.CurrentSchema;

			// Does the user have privs to alter this tables?
			if (!Connection.Database.CanUserAlterTableObject(context, User, tname)) {
				throw new UserAccessException("User not permitted to alter table: " + table_name);
			}

			if (create_statement != null) {
				// Create the data table definition and tell the database to update it.
				DataTableDef table_def = create_stmt.CreateDataTableDef();
				TableName tname1 = table_def.TableName;
				// Is the table in the database already?
				if (Connection.TableExists(tname1)) {
					// Drop any schema for this table,
					Connection.DropAllConstraintsForTable(tname1);
					Connection.UpdateTable(table_def);
				}
					// If the table isn't in the database,
				else {
					Connection.CreateTable(table_def);
				}

				// Setup the constraints
				create_stmt.SetupAllConstraints();

				// Return '0' if we created the table.
				return FunctionTable.ResultTable(context, 0);
			} else {
				// SQL alter command using the alter table actions,

				// Get the table definition for the table name,
				DataTableDef table_def = Connection.GetTable(tname).DataTableDef;
				String table_name = table_def.Name;
				DataTableDef new_table = table_def.NoColumnCopy();

				// Returns a ColumnChecker implementation for this table.
				ColumnChecker checker =
					ColumnChecker.GetStandardColumnChecker(Connection, tname);

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
						if (action.Action == AlterTableActionType.SetDefault &&
						    CheckColumnNamesMatch(Connection, (String) action.Elements[0], col_name)) {
							Expression exp = (Expression) action.Elements[1];
							checker.CheckExpression(exp);
							column.SetDefaultExpression(exp);
							table_altered = true;
						} else if (action.Action == AlterTableActionType.DropDefault &&
						           CheckColumnNamesMatch(Connection, (String) action.Elements[0], col_name)) {
							column.SetDefaultExpression(null);
							table_altered = true;
						} else if (action.Action == AlterTableActionType.DropColumn &&
						           CheckColumnNamesMatch(Connection, (String) action.Elements[0], col_name)) {
							// Check there are no referential links to this column
							Transaction.ColumnGroupReference[] refs = Connection.QueryTableImportedForeignKeyReferences(tname);
							for (int p = 0; p < refs.Length; ++p) {
								CheckColumnConstraint(col_name, refs[p].ref_columns, refs[p].ref_table_name, refs[p].name);
							}
							// Or from it
							refs = Connection.QueryTableForeignKeyReferences(tname);
							for (int p = 0; p < refs.Length; ++p) {
								CheckColumnConstraint(col_name, refs[p].key_columns, refs[p].key_table_name, refs[p].name);
							}
							// Or that it's part of a primary key
							Transaction.ColumnGroup primary_key =
								Connection.QueryTablePrimaryKeyGroup(tname);
							if (primary_key != null) {
								CheckColumnConstraint(col_name, primary_key.columns, tname, primary_key.name);
							}
							// Or that it's part of a unique set
							Transaction.ColumnGroup[] uniques =
								Connection.QueryTableUniqueGroups(tname);
							for (int p = 0; p < uniques.Length; ++p) {
								CheckColumnConstraint(col_name, uniques[p].columns, tname, uniques[p].name);
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
					if (action.Action == AlterTableActionType.AddColumn) {
						SqlColumn cdef = (SqlColumn) action.Elements[0];
						if (cdef.IsUnique || cdef.IsPrimaryKey) {
							throw new DatabaseException("Can not use UNIQUE or PRIMARY KEY " +
							                            "column constraint when altering a column.  Use " +
							                            "ADD CONSTRAINT instead.");
						}
						// Convert to a DataTableColumnDef
						DataTableColumnDef col = CreateTableStatement.ConvertColumnDef(cdef);

						checker.CheckExpression(
							col.GetDefaultExpression(Connection.System));
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
					if (action.Action == AlterTableActionType.DropConstraint) {
						String constraint_name = (String) action.Elements[0];
						int drop_count = Connection.DropNamedConstraint(tname, constraint_name);
						if (drop_count == 0) {
							throw new DatabaseException(
								"Named constraint to drop on table " + tname +
								" was not found: " + constraint_name);
						}
					} else if (action.Action == AlterTableActionType.DropPrimaryKey) {
						bool constraint_dropped = Connection.DropPrimaryKeyConstraintForTable(tname, null);
						if (!constraint_dropped) {
							throw new DatabaseException("No primary key to delete on table " + tname);
						}
					}
				}

				// Any constraints to add...
				for (int i = 0; i < actions.Count; ++i) {
					AlterTableAction action = (AlterTableAction) actions[i];
					if (action.Action == AlterTableActionType.AddConstraint) {
						SqlConstraint constraint = (SqlConstraint) action.Elements[0];
						bool foreign_constraint = (constraint.Type == ConstraintType.ForeignKey);
						TableName ref_tname = null;
						if (foreign_constraint) {
							ref_tname =
								ResolveTableName(constraint.ReferenceTable, Connection);
							if (Connection.IsInCaseInsensitiveMode) {
								ref_tname = Connection.TryResolveCase(ref_tname);
							}
							constraint.ReferenceTable = ref_tname.ToString();
						}

						checker.StripColumnList(table_name, constraint.column_list);
						checker.StripColumnList(constraint.ReferenceTable,
						                        constraint.column_list2);
						checker.CheckExpression(constraint.CheckExpression);
						checker.CheckColumnList(constraint.column_list);
						if (foreign_constraint && constraint.column_list2 != null) {
							ColumnChecker referenced_checker =
								ColumnChecker.GetStandardColumnChecker(Connection, ref_tname);
							referenced_checker.CheckColumnList(constraint.column_list2);
						}

						CreateTableStatement.AddSchemaConstraint(Connection, tname, constraint);

					}
				}

				// Alter the existing table to the new format...
				if (table_altered) {
					if (new_table.ColumnCount == 0) {
						throw new DatabaseException("Can not ALTER table to have 0 columns.");
					}
					Connection.UpdateTable(new_table);
				} else {
					// If the table wasn't physically altered, check the constraints.
					// Calling this method will also make the transaction check all
					// deferred constraints during the next commit.
					Connection.CheckAllConstraints(tname);
				}

				// Return '0' if everything successful.
				return FunctionTable.ResultTable(context, 0);
			}
		}
	}
}