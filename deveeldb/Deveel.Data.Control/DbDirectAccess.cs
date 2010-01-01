//  
//  DbDirectAccess.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Text;

using Deveel.Data.Client;
using Deveel.Data.Procedures;
using Deveel.Data.Sql;
using Deveel.Diagnostics;

namespace Deveel.Data.Control {
	/// <summary>
	/// Defines a commonly accessible method to interact with the underlying
	/// database by executing commands through an authorized connection.
	/// </summary>
	public sealed class DbDirectAccess : IDisposable, DatabaseConnection.CallBack {
		internal DbDirectAccess(DbSystem system, User user, string defaultSchema) {
			Connect(system, user, defaultSchema);

			context = new DatabaseQueryContext(connection);
		}

		internal DbDirectAccess(DatabaseConnection connection) {
			//this constructor skips the connect process...
			this.connection = connection;

			context = new DatabaseQueryContext(connection);
		}

		private DatabaseConnection connection;
		private readonly DatabaseQueryContext context;
		private readonly ArrayList table_list;

		/// <summary>
		/// The event raised when a trigger is fired within the current
		/// underlying connection to the database.
		/// </summary>
		public event EventHandler TriggerEvent;

		public event EventHandler CommandExecuting;
		public event EventHandler CommandExecuted;
		public event EventHandler CommandError;

		/// <summary>
		/// Gets the user owner of the current underlying connection.
		/// </summary>
		public User User {
			get { return connection.User; }
		}

		private void Connect(DbSystem system, User user, string defaultSchema) {
			connection = system.Database.CreateNewConnection(user, this);

			// Put the connection in exclusive mode
			LockingMechanism locker = connection.LockingMechanism;
			locker.SetMode(LockingMode.Exclusive);
			try {
				connection.AutoCommit = true;

				// Set the default schema for this connection if it exists
				if (connection.SchemaExists(defaultSchema)) {
					connection.SetDefaultSchema(defaultSchema);
				} else {
					connection.Debug.Write(DebugLevel.Warning, this, "Couldn't change to '" + defaultSchema + "' schema.");
					// If we can't change to the schema then change to the APP schema
					connection.SetDefaultSchema("APP");
				}
			} finally {
				try {
					// Make sure we commit the connection.
					connection.Commit();
				} catch (TransactionException e) {
					// Just issue a warning...
					connection.Debug.WriteException(DebugLevel.Warning, e);
				} finally {
					// Guarentee that we unluck from EXCLUSIVE
					locker.FinishMode(LockingMode.Exclusive);
				}
			}
		}

		#region Method Implementations

		private object ExecuteCommandImpl(DirectCommand command) {
			object result = null;

			switch (command.CommandType) {
				case DirectCommandType.CreateSchema:
					CreateSchemaImpl((string)command.GetRequired("schema"));
					break;
				case DirectCommandType.DropSchema:
					DropSchemaImpl((string)command.GetRequired("schema"));
					break;
				case DirectCommandType.CreateTable:
					CreateTableImpl((DataTableDef)command.GetRequired("table"), (bool)command.GetOptional("if_not_exists", false));
					break;
				case DirectCommandType.AlterTable:
					AlterTableImpl((TableName)command.GetRequired("table_name"), (IList)command.GetRequired("actions"));
					break;
				case DirectCommandType.AlterCreateTable:
					AlterCreateTableImpl((DataTableDef)command.GetRequired("table"));
					break;
				case DirectCommandType.DropTables:
					result = DropTablesImpl((TableName[]) command.GetRequired("table_names"), (bool) command.GetOptional("only_if_exist", false));
					break;
				case DirectCommandType.CreateView:
					CreateViewImpl((TableName) command.GetRequired("name"), (string[]) command.GetOptional("columns", null),
					               (TableSelectExpression) command.GetRequired("select"));
					break;
				case DirectCommandType.DropView:
					DropViewImpl((TableName)command.GetRequired("name"));
					break;
				case DirectCommandType.CreateTrigger: {
					object name = command.GetRequired("name");
					TableName onTable = (TableName) command.GetRequired("table_name");
					TriggerEventType eventType = (TriggerEventType) command.GetRequired("event_type");
					ProcedureName procedureName = (ProcedureName) command.GetOptional("procedure", null);
					Expression[] procedureArgs = (Expression[]) command.GetOptional("args", null);

					if (procedureName != null) {
						CreateProcedureTriggerImpl((TableName)name, onTable, eventType, procedureName, procedureArgs);
					} else {
						CreateCallbackTriggerImpl((string)name, onTable, eventType);
					}
					break;
				}
				case DirectCommandType.DropTrigger: {
					object name = command.GetRequired("name");
					if (name is TableName) {
						DropProcedureTriggerImpl((TableName)name);
					} else {
						DropCallbackTriggerImpl((string)name);
					}
					break;
				}

				case DirectCommandType.Call:
					result = CallImpl((ProcedureName) command.GetRequired("name"), (Expression[]) command.GetOptional("args", null));
					break;

				case DirectCommandType.Select:
					result = SelectImpl((TableSelectExpression) command.GetRequired("expression"),
					                    (ByColumn[]) command.GetOptional("order_by", null));
					break;
				case DirectCommandType.Insert: {
					TableName tableName = (TableName) command.GetRequired("table_name");
					Assignment[] assignments = (Assignment[]) command.GetOptional("assignments", null);
					IList values = (IList) command.GetOptional("values", null);
					if (assignments != null) {
						InsertImpl(tableName, assignments);
						result = 1;
					} else if (values != null) {
						string[] columns = (string[]) command.GetOptional("columns", null);
						result = InsertImpl(tableName, columns, values);
					}
					break;
				}
				case DirectCommandType.Delete:
					result = DeleteImpl((TableName) command.GetRequired("table_name"),
					                    (Expression) command.GetRequired("search_expression"), (int) command.GetOptional("limit", -1));
					break;
				case DirectCommandType.Update:
					result = UpdateImpl((TableName) command.GetRequired("table_name"),
					                    (Assignment[]) command.GetRequired("assignments"),
					                    (Expression) command.GetRequired("search_expression"), (int) command.GetOptional("limit", -1));
					break;
				case DirectCommandType.Commit:
					CommitImpl();
					break;
				case DirectCommandType.Rollback:
					RollbackImpl();
					break;
				default:
					throw new NotImplementedException("The support for command type '" + command.CommandType + "' is not supported.");
			}

			return result;
		}

		#region Schemata Management

		private void CreateSchemaImpl(string name) {
			if (!connection.Database.CanUserCreateAndDropSchema(context, connection.User, name))
				throw new UserAccessException("User not permitted to create or drop schema.");

			bool ignore_case = connection.IsInCaseInsensitiveMode;
			SchemaDef schema = connection.ResolveSchemaCase(name, ignore_case);
			if (schema != null)
				throw new DatabaseException("Schema '" + name + "' already exists.");

			// Create the schema
			connection.CreateSchema(name, "USER");
			// Set the default grants for the schema
			connection.GrantManager.Grant(Privileges.SchemaAll, GrantObject.Schema, name, connection.User.UserName, true,
										  Database.InternalSecureUsername);
		}

		private void DropSchemaImpl(string name) {
			if (!connection.Database.CanUserCreateAndDropSchema(context, connection.User, name))
				throw new UserAccessException("User not permitted to create or drop schema.");

			bool ignore_case = connection.IsInCaseInsensitiveMode;
			SchemaDef schema = connection.ResolveSchemaCase(name, ignore_case);
			// Only allow user to drop USER typed schemas
			if (schema == null)
				throw new DatabaseException("Schema '" + name + "' does not exist.");

			if (!schema.Type.Equals("USER"))
				throw new DatabaseException("Can not drop schema '" + name + "'");

			// Check if the schema is empty.
			TableName[] all_tables = connection.Tables;
			String resolved_schema_name = schema.Name;
			for (int i = 0; i < all_tables.Length; ++i) {
				if (all_tables[i].Schema.Equals(resolved_schema_name))
					throw new DatabaseException("Schema '" + name + "' is not empty.");
			}
			// Drop the schema
			connection.DropSchema(schema.Name);
			// Revoke all the grants for the schema
			connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Schema, schema.Name);
		}

		#endregion

		#region Tables Management

		private void CreateTableImpl(DataTableDef tableDef, bool ifNotExists) {
			TableName tableName = ResolveName(tableDef.TableName);

			// Does the schema exist?
			bool ignore_case = connection.IsInCaseInsensitiveMode;
			SchemaDef schema = connection.ResolveSchemaCase(tableName.Schema, ignore_case);
			if (schema == null)
				throw new DatabaseException("Schema '" + tableName.Schema + "' doesn't exist.");

			// Does the user have privs to create this tables?
			if (!connection.Database.CanUserCreateTableObject(context, connection.User, tableName))
				throw new UserAccessException("User not permitted to create table: " + tableDef.TableName);

			// Does the table already exist?
			if (connection.TableExists(tableName)) {
				if (!ifNotExists)
					throw new InvalidOperationException("The table '" + tableDef.TableName +
														"' already exists and IF NOT EXISTS clause was not specified.");
				return;
			}

			tableDef.TableName = tableName;
			tableDef.TableType = "Deveel.Data.VariableSizeDataTableFile";

			// Create the data table definition and tell the database to create
			// it.
			connection.CreateTable(tableDef);

			// The initial grants for a table is to give the user who created it
			// full access.
			connection.GrantManager.Grant(Privileges.TableAll, GrantObject.Table, tableName.ToString(),
										  connection.User.UserName, true, Database.InternalSecureUsername);
		}

		private void AlterTableImpl(TableName tableName, IList actions) {
			// Get the table definition for the table name,
			DataTableDef table_def = connection.GetTable(tableName).DataTableDef;
			String table_name = table_def.Name;
			DataTableDef new_table = table_def.NoColumnCopy();

			// Returns a ColumnChecker implementation for this table.
			ColumnChecker checker = ColumnChecker.GetStandardColumnChecker(connection, tableName);

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
					AlterTableAction action = (AlterTableAction)actions[i];
					if (action.Action == AlterTableActionType.SetDefault &&
						CheckColumnNamesMatch((string)action.Elements[0], col_name)) {
						Expression exp = (Expression)action.Elements[1];
						checker.CheckExpression(exp);
						column.SetDefaultExpression(exp);
						table_altered = true;
					} else if (action.Action == AlterTableActionType.DropDefault &&
							   CheckColumnNamesMatch((string)action.Elements[0], col_name)) {
						column.SetDefaultExpression(null);
						table_altered = true;
					} else if (action.Action == AlterTableActionType.DropColumn &&
							   CheckColumnNamesMatch((string)action.Elements[0], col_name)) {
						// Check there are no referential links to this column
						Transaction.ColumnGroupReference[] refs = connection.QueryTableImportedForeignKeyReferences(tableName);
						for (int p = 0; p < refs.Length; ++p) {
							CheckColumnConstraint(col_name, refs[p].ref_columns, refs[p].ref_table_name, refs[p].name);
						}
						// Or from it
						refs = connection.QueryTableForeignKeyReferences(tableName);
						for (int p = 0; p < refs.Length; ++p) {
							CheckColumnConstraint(col_name, refs[p].key_columns, refs[p].key_table_name, refs[p].name);
						}
						// Or that it's part of a primary key
						Transaction.ColumnGroup primary_key = connection.QueryTablePrimaryKeyGroup(tableName);
						if (primary_key != null) {
							CheckColumnConstraint(col_name, primary_key.columns, tableName, primary_key.name);
						}
						// Or that it's part of a unique set
						Transaction.ColumnGroup[] uniques = connection.QueryTableUniqueGroups(tableName);
						for (int p = 0; p < uniques.Length; ++p) {
							CheckColumnConstraint(col_name, uniques[p].columns, tableName, uniques[p].name);
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
				AlterTableAction action = (AlterTableAction)actions[i];
				if (action.Action == AlterTableActionType.AddColumn) {
					DataTableColumnDef cdef = (DataTableColumnDef)action.Elements[0];
					/*
					TODO: check this ...
					if (cdef.IsUnique || cdef.IsPrimaryKey)
						throw new DatabaseException("Can not use UNIQUE or PRIMARY KEY " +
													"column constraint when altering a column.  Use " +
													"ADD CONSTRAINT instead.");
					*/

					checker.CheckExpression(cdef.GetDefaultExpression(connection.System));
					string col_name = cdef.Name;
					// If column name starts with [table_name]. then strip it off
					cdef.Name = checker.StripTableName(table_name, col_name);
					new_table.AddColumn(cdef);
					table_altered = true;
				}
			}

			// Any constraints to drop...
			for (int i = 0; i < actions.Count; ++i) {
				AlterTableAction action = (AlterTableAction)actions[i];
				if (action.Action == AlterTableActionType.DropConstraint) {
					String constraint_name = (String)action.Elements[0];
					int drop_count = connection.DropNamedConstraint(tableName, constraint_name);
					if (drop_count == 0)
						throw new DatabaseException("Named constraint to drop on table " + tableName + " was not found: " +
													constraint_name);
				} else if (action.Action == AlterTableActionType.DropPrimaryKey) {
					bool constraint_dropped = connection.DropPrimaryKeyConstraintForTable(tableName, null);
					if (!constraint_dropped)
						throw new DatabaseException("No primary key to delete on table " + tableName);
				}
			}

			// Any constraints to add...
			for (int i = 0; i < actions.Count; ++i) {
				AlterTableAction action = (AlterTableAction)actions[i];
				if (action.Action == AlterTableActionType.AddConstraint) {
					DataTableConstraintDef constraint = (DataTableConstraintDef)action.Elements[0];
					bool foreign_constraint = (constraint.Type == ConstraintType.ForeignKey);
					TableName ref_tname = null;
					if (foreign_constraint) {
						ref_tname = connection.ResolveTableName(constraint.ReferencedTableName);
						if (connection.IsInCaseInsensitiveMode)
							ref_tname = connection.TryResolveCase(ref_tname);
						constraint.SetReferencedTableName(ref_tname.ToString());
					}

					checker.StripColumnList(table_name, constraint.ColumnsList);
					checker.StripColumnList(constraint.ReferencedTableName, constraint.ReferencedColumnsList);
					checker.CheckExpression(constraint.CheckExpression);
					checker.CheckColumnList(constraint.ColumnsList);
					if (foreign_constraint && constraint.ReferencedColumns != null) {
						ColumnChecker referenced_checker = ColumnChecker.GetStandardColumnChecker(connection, ref_tname);
						referenced_checker.CheckColumnList(constraint.ReferencedColumnsList);
					}

					AddConstraintImpl(tableName, constraint);
				}
			}

			// Alter the existing table to the new format...
			if (table_altered) {
				if (new_table.ColumnCount == 0)
					throw new DatabaseException("Can not ALTER table to have 0 columns.");

				connection.UpdateTable(new_table);
			} else {
				// If the table wasn't physically altered, check the constraints.
				// Calling this method will also make the transaction check all
				// deferred constraints during the next commit.
				connection.CheckAllConstraints(tableName);
			}
		}

		private void AlterCreateTableImpl(DataTableDef tableDef) {
			// Does the user have privs to alter this tables?
			if (!connection.Database.CanUserAlterTableObject(context, connection.User, tableDef.TableName))
				throw new UserAccessException("User not permitted to alter table: " + tableDef.TableName);

			TableName tableName = tableDef.TableName;
			// Is the table in the database already?
			if (connection.TableExists(tableName)) {
				// Drop any schema for this table,
				connection.DropAllConstraintsForTable(tableName);
				connection.UpdateTable(tableDef);
			}
				// If the table isn't in the database,
			else {
				connection.CreateTable(tableDef);
			}

			// Any pending constraints have to be setup after this
		}

		private int DropTablesImpl(TableName[] tableNames, bool onlyIfExist) {
			// Check there are no duplicate entries in the list of tables to drop
			for (int i = 0; i < tableNames.Length; ++i) {
				TableName check = tableNames[i];
				for (int n = i + 1; n < tableNames.Length; ++n) {
					if (tableNames[n].Equals(check))
						throw new ArgumentException("Duplicate table in drop: " + check);
				}
			}

			int list_size = tableNames.Length;
			ArrayList resolved_tables = new ArrayList(list_size);
			// Check the user has privs to delete these tables...
			for (int i = 0; i < list_size; ++i) {
				String table_name = tableNames[i].ToString();
				TableName tname = connection.ResolveTableName(table_name);
				// Does the table exist?
				if (!onlyIfExist && !connection.TableExists(tname))
					throw new DatabaseException("Table '" + tname + "' does not exist.");

				resolved_tables.Add(tname);
				// Does the user have privs to drop this tables?
				if (!connection.Database.CanUserDropTableObject(context, connection.User, tname))
					throw new UserAccessException("User not permitted to drop table: " + tname);
			}

			// Check there are no referential links to any tables being dropped
			for (int i = 0; i < list_size; ++i) {
				TableName tname = (TableName)resolved_tables[i];
				// Any tables that have a referential link to this table.
				Transaction.ColumnGroupReference[] refs = connection.QueryTableImportedForeignKeyReferences(tname);
				for (int n = 0; n < refs.Length; ++n) {
					// If the key table isn't being dropped then error
					if (!resolved_tables.Contains(refs[n].key_table_name)) {
						throw new DatabaseConstraintViolationException(
						  DatabaseConstraintViolationException.DropTableViolation,
							"Constraint violation (" + refs[n].name + ") dropping table " +
							tname + " because of referential link from " +
							refs[n].key_table_name);
					}
				}
			}


			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!onlyIfExist) {
				// For each table to drop.
				for (int i = 0; i < list_size; ++i) {
					// Does the table already exist?
					TableName tname = (TableName)resolved_tables[i];

					// If table doesn't exist, throw an error
					if (!connection.TableExists(tname))
						throw new DatabaseException("Can not drop table '" + tname + "'.  It does not exist.");
				}
			}

			// For each table to drop.
			int droppedTableCount = 0;
			GrantManager grant_manager = connection.GrantManager;
			for (int i = 0; i < list_size; ++i) {
				// Does the table already exist?
				TableName tname = (TableName)resolved_tables[i];
				if (connection.TableExists(tname)) {
					// Drop table in the transaction
					connection.DropTable(tname);
					// Drop the grants for this object
					grant_manager.RevokeAllGrantsOnObject(GrantObject.Table, tname.ToString());
					// Drop all constraints from the schema
					connection.DropAllConstraintsForTable(tname);
					++droppedTableCount;
				}
			}

			return droppedTableCount;
		}

		private void AddConstraintImpl(TableName tableName, DataTableConstraintDef constraint) {
			tableName = ResolveName(tableName);

			if (constraint.Type == ConstraintType.PrimaryKey) {
				connection.AddPrimaryKeyConstraint(tableName, constraint.Columns, constraint.Deferred, constraint.Name);
			} else if (constraint.Type == ConstraintType.ForeignKey) {
				// Currently we forbid referencing a table in another schema
				TableName ref_table = TableName.Resolve(constraint.ReferencedTableName);
				ConstraintAction update_rule = constraint.UpdateRule;
				ConstraintAction delete_rule = constraint.DeleteRule;
				if (!tableName.Schema.Equals(ref_table.Schema))
					throw new DatabaseException("Foreign key reference error: " +
												"Not permitted to reference a table outside of the schema: " +
												tableName + " -> " + ref_table);

				connection.AddForeignKeyConstraint(tableName, constraint.Columns, ref_table, constraint.ReferencedColumns,
												   delete_rule, update_rule, constraint.Deferred, constraint.Name);
			} else if (constraint.Type == ConstraintType.Unique) {
				connection.AddUniqueConstraint(tableName, constraint.Columns, constraint.Deferred, constraint.Name);
			} else if (constraint.Type == ConstraintType.Check) {
				connection.AddCheckConstraint(tableName, constraint.original_check_expression, constraint.Deferred, constraint.Name);
			} else {
				throw new DatabaseException("Unrecognized constraint type.");
			}
		}

		#endregion

		#region Views Management

		private void CreateViewImpl(TableName name, string[] columnNames, TableSelectExpression selectExpression) {
			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(selectExpression, connection);
			// Form the plan
			IQueryPlanNode plan = Planner.FormQueryPlan(connection, selectExpression, from_set, new ArrayList());

			// Wrap the result around a SubsetNode to alias the columns in the
			// table correctly for this view.
			int sz = (columnNames == null) ? 0 : columnNames.Length;
			VariableName[] original_vars = from_set.GenerateResolvedVariableList();
			VariableName[] new_column_vars = new VariableName[original_vars.Length];

			if (sz > 0) {
				if (sz != original_vars.Length)
					throw new StatementException("Column list is not the same size as the columns selected.");

				for (int i = 0; i < sz; ++i) {
					String col_name = columnNames[i];
					new_column_vars[i] = new VariableName(name, col_name);
				}
			} else {
				sz = original_vars.Length;
				for (int i = 0; i < sz; ++i) {
					new_column_vars[i] = new VariableName(name, original_vars[i].Name);
				}
			}

			// Check there are no repeat column names in the table.
			for (int i = 0; i < sz; ++i) {
				VariableName cur_v = new_column_vars[i];
				for (int n = i + 1; n < sz; ++n) {
					if (new_column_vars[n].Equals(cur_v)) {
						throw new DatabaseException("Duplicate column name '" + cur_v + "' in view.  " +
													"A view may not contain duplicate column names.");
					}
				}
			}

			// Wrap the plan around a SubsetNode plan
			plan = new QueryPlan.SubsetNode(plan, original_vars, new_column_vars);

			// Does the user have privs to create this tables?
			if (!connection.Database.CanUserCreateTableObject(context, connection.User, name))
				throw new UserAccessException("User not permitted to create view: " + name);

			// Does the schema exist?
			bool ignore_case = connection.IsInCaseInsensitiveMode;
			SchemaDef schema = connection.ResolveSchemaCase(name.Schema, ignore_case);
			if (schema == null)
				throw new DatabaseException("Schema '" + name.Schema + "' doesn't exist.");

			name = new TableName(schema.Name, name.Name);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			CheckUserSelectPermissions(plan);

			// Does the table already exist?
			if (connection.TableExists(name))
				throw new DatabaseException("View or table with name '" + name + "' already exists.");

			// Before evaluation, make a clone of the plan,
			IQueryPlanNode plan_copy;
			try {
				plan_copy = (IQueryPlanNode)plan.Clone();
			} catch (Exception e) {
				connection.Debug.WriteException(e);
				throw new DatabaseException("Clone error: " + e.Message);
			}

			// We have to execute the plan to get the DataTableDef that represents the
			// result of the view execution.
			Table t = plan.Evaluate(context);
			DataTableDef data_table_def = new DataTableDef(t.DataTableDef);
			data_table_def.TableName = name;

			// Create a ViewDef object,
			ViewDef view_def = new ViewDef(data_table_def, plan_copy);

			// And create the view object,
			//TODO: check the .ToString() method to return the correct SQL syntax...
			connection.CreateView(new SqlQuery(selectExpression.ToString()), view_def);

			// The initial grants for a view is to give the user who created it
			// full access.
			connection.GrantManager.Grant(
				 Privileges.TableAll, GrantObject.Table, name.ToString(),
				 connection.User.UserName, true, Database.InternalSecureUsername);
		}

		private void DropViewImpl(TableName name) {
			// Does the user have privs to drop this tables?
			if (!connection.Database.CanUserDropTableObject(context, connection.User, name))
				throw new UserAccessException("User not permitted to drop view: " + name);

			// Drop the view object
			connection.DropView(name);

			// Drop the grants for this object
			connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, name.ToString());
		}

		#endregion

		#region Triggers Management

		private void CreateCallbackTriggerImpl(string name, TableName tableName, TriggerEventType eventType) {
			if ((eventType & TriggerEventType.After) != 0 ||
				(eventType & TriggerEventType.Before) != 0)
				throw new ArgumentException("Invalid event type for a callback trigger.");

			connection.CreateTrigger(name, tableName.ToString(), eventType);
		}

		private void DropCallbackTriggerImpl(string name) {
			connection.DeleteTrigger(name);
		}

		private void CreateProcedureTriggerImpl(TableName name, TableName tableName, TriggerEventType eventType, ProcedureName procedure, Expression[] args) {
			// Get the procedure manager
			ProcedureManager proc_manager = connection.ProcedureManager;

			// Check the trigger name doesn't clash with any existing database object.
			if (connection.TableExists(name))
				throw new DatabaseException("A database object with name '" + name + "' already exists.");

			// Check the procedure exists.
			if (!proc_manager.ProcedureExists(procedure))
				throw new DatabaseException("Procedure '" + procedure + "' could not be found.");

			// Resolve the procedure arguments,
			TObject[] vals = new TObject[args.Length];
			for (int i = 0; i < args.Length; ++i)
				vals[i] = args[i].Evaluate(null, null, context);

			// Create the trigger,
			ConnectionTriggerManager manager = connection.ConnectionTriggerManager;
			manager.CreateTableTrigger(name.Schema, name.Name, eventType, tableName, procedure.ToString(), vals);

			// The initial grants for a trigger is to give the user who created it
			// full access.
			connection.GrantManager.Grant(Privileges.ProcedureAll, GrantObject.Table, name.ToString(), connection.User.UserName,
										  true, Database.InternalSecureUsername);
		}

		private void DropProcedureTriggerImpl(TableName name) {
			ConnectionTriggerManager manager = connection.ConnectionTriggerManager;
			manager.DropTrigger(name.Schema, name.Name);

			// Drop the grants for this object
			connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, name.ToString());
		}

		#endregion

		#region Procedures Management

		private TObject CallImpl(ProcedureName procedureName, Expression[] args) {
			ProcedureManager manager = connection.ProcedureManager;

			// If this doesn't exist then generate the error
			if (!manager.ProcedureExists(procedureName))
				throw new DatabaseException("Stored procedure '" + procedureName + "' was not found.");

			// Check the user has privs to use this stored procedure
			if (!connection.Database.CanUserExecuteStoredProcedure(context, connection.User, procedureName.ToString()))
				throw new UserAccessException("User not permitted to call: " + procedureName);

			// Evaluate the arguments
			TObject[] vals = new TObject[args.Length];
			for (int i = 0; i < args.Length; ++i) {
				if (args[i].IsConstant) {
					vals[i] = args[i].Evaluate(null, null, context);
				} else {
					throw new StatementException("CALL argument is not a constant: " + args[i].Text);
				}
			}

			// Invoke the procedure
			return manager.InvokeProcedure(procedureName, vals);
		}

		#endregion

		#region Select

		private Table SelectImpl(TableSelectExpression selectExpression, ByColumn[] orderBy) {
			// check to see if the construct is the special one for
			// selecting the latest IDENTITY value from a table
			if (IsIdentitySelect(selectExpression)) {
				selectExpression.Columns.RemoveAt(0);
				SelectColumn curValFunction = new SelectColumn();

				FromTable from_table = (FromTable)((ArrayList)selectExpression.From.AllTables)[0];
				curValFunction.SetExpression(Expression.Parse("IDENTITY('" + from_table.Name + "')"));
				curValFunction.SetAlias("IDENTITY");
				selectExpression.Columns.Add(curValFunction);
			}

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(selectExpression, connection);

			// Form the plan
			IQueryPlanNode plan = Planner.FormQueryPlan(connection, selectExpression, from_set, orderBy);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			CheckUserSelectPermissions(plan);

			bool error = true;
			try {
				Table t = plan.Evaluate(context);

				if (selectExpression.Into.HasElements)
					t = selectExpression.Into.SelectInto(context, t);

				error = false;
				return t;
			} finally {
				// If an error occured, dump the command plan to the debug log.
				// Or just dump the command plan if debug level = Information
				if (connection.Debug.IsInterestedIn(DebugLevel.Information) ||
					(error && connection.Debug.IsInterestedIn(DebugLevel.Warning))) {
					StringBuilder buf = new StringBuilder();
					plan.DebugString(0, buf);

					connection.Debug.Write(DebugLevel.Warning, this, "Query Plan debug:\n" + buf);
				}
			}
		}

		#endregion

		#region Delete

		private int DeleteImpl(TableName tableName, Expression searchExpression, int limit) {
			// Does the table exist?
			if (!connection.TableExists(tableName))
				throw new DatabaseException("Table '" + tableName + "' does not exist.");

			// Form a TableSelectExpression that represents the select on the table
			TableSelectExpression selectExpression = new TableSelectExpression();
			// Create the FROM clause
			selectExpression.From.AddTable(tableName.ToString());
			// Set the WHERE clause
			selectExpression.Where = new SearchExpression(searchExpression);

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(selectExpression, connection);
			// Form the plan
			IQueryPlanNode plan = Planner.FormQueryPlan(connection, selectExpression, from_set, null);

			// Get the table we are updating
			DataTable updateTable = connection.GetTable(tableName);

			// Resolve all tables linked to this
			TableName[] linked_tables = connection.QueryTablesRelationallyLinkedTo(tableName);
			ArrayList relationallyLinkedTables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationallyLinkedTables.Add(connection.GetTable(linked_tables[i]));
			}

			// Check that this user has privs to delete from the table.
			if (!connection.Database.CanUserDeleteFromTableObject(context, connection.User, tableName))
				throw new UserAccessException("User not permitted to delete from table: " + tableName);

			// Check the user has select permissions on the tables in the plan.
			CheckUserSelectPermissions(plan);

			// Evaluates the delete statement...

			// Evaluate the plan to find the update set.
			Table delete_set = plan.Evaluate(context);

			// Delete from the data table.
			int delete_count = updateTable.Delete(delete_set, limit);

			// Notify TriggerManager that we've just done an update.
			if (delete_count > 0)
				connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Delete, tableName.ToString(), delete_count));

			return delete_count;
		}

		#endregion

		#region Insert

		private void InsertImpl(TableName tableName, Assignment[] assignments) {
			// Does the table exist?
			if (!connection.TableExists(tableName))
				throw new DatabaseException("Table '" + tableName + "' does not exist.");

			VariableName[] col_var_list = new VariableName[assignments.Length];
			for (int i = 0; i < assignments.Length; i++)
				col_var_list[i] = ResolveColumn(assignments[i].VariableName);

			// Add the from table direct source for this table
			ITableQueryDef table_query_def = connection.GetTableQueryDef(tableName, null);
			AddTable(new FromTableDirectSource(connection.IsInCaseInsensitiveMode, table_query_def, "INSERT_TABLE", tableName, tableName));

			// Get the table we are inserting to
			DataTable insert_table = connection.GetTable(tableName);

			// If there's a sub select in an expression in the 'SET' clause then
			// throw an error.
			for (int i = 0; i < assignments.Length; ++i) {
				Assignment assignment = assignments[i];
				Expression exp = assignment.Expression;
				IList elem_list = exp.AllElements;
				for (int n = 0; n < elem_list.Count; ++n) {
					object ob = elem_list[n];
					if (ob is SelectStatement) {
						throw new DatabaseException("Illegal to have sub-select in SET clause.");
					}
				}

				// Resolve the column names in the columns set.
				VariableName v = assignment.VariableName;
				VariableName resolved_v = ResolveColumn(v);
				v.Set(resolved_v);
				ResolveExpression(assignment.Expression);
			}

			// Resolve all tables linked to this
			TableName[] linked_tables = connection.QueryTablesRelationallyLinkedTo(tableName);
			ArrayList relationallyLinkedTables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i)
				relationallyLinkedTables.Add(connection.GetTable(linked_tables[i]));

			// Check that this user has privs to insert into the table.
			if (!connection.Database.CanUserInsertIntoTableObject(context, User, tableName, col_var_list))
				throw new UserAccessException("User not permitted to insert in to table: " + tableName);

			// Insert rows from the set assignments.
			DataRow dataRow = insert_table.NewRow();
			Assignment[] assigns = new Assignment[assignments.Length];
			assignments.CopyTo(assigns, 0);
			dataRow.SetupEntire(assigns, context);
			insert_table.Add(dataRow);

			// Notify TriggerManager that we've just done an update.
			connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Insert, tableName.ToString(), 1));
		}

		private int InsertImpl(TableName tableName, string[] columns, IList values) {
			// Check 'values_list' contains all same size size insert element arrays.
			int first_len = -1;
			for (int n = 0; n < values.Count; ++n) {
				IList exp_list = (IList)values[n];
				if (first_len == -1 || first_len == exp_list.Count) {
					first_len = exp_list.Count;
				} else {
					throw new DatabaseException("The insert data list varies in size.");
				}
			}

			// Does the table exist?
			if (!connection.TableExists(tableName))
				throw new DatabaseException("Table '" + tableName + "' does not exist.");

			// Add the from table direct source for this table
			ITableQueryDef table_query_def = connection.GetTableQueryDef(tableName, null);
			AddTable(new FromTableDirectSource(connection.IsInCaseInsensitiveMode, table_query_def, "INSERT_TABLE", tableName, tableName));

			// Get the table we are inserting to
			DataTable insert_table = connection.GetTable(tableName);

			ArrayList col_list = new ArrayList(columns);

			// If 'col_list' is empty we must pick every entry from the insert
			// table.
			if (col_list.Count == 0) {
				for (int i = 0; i < insert_table.ColumnCount; ++i) {
					col_list.Add(insert_table.GetColumnDef(i).Name);
				}
			}
			// Resolve 'col_list' into a list of column indices into the insert
			// table.
			int[] col_index_list = new int[col_list.Count];
			VariableName[] col_var_list = new VariableName[col_list.Count];
			for (int i = 0; i < col_list.Count; ++i) {
				VariableName in_var = VariableName.Resolve((String)col_list[i]);
				VariableName col = ResolveColumn(in_var);
				int index = insert_table.FastFindFieldName(col);
				if (index == -1) {
					throw new DatabaseException("Can't find column: " + col);
				}
				col_index_list[i] = index;
				col_var_list[i] = col;
			}

			// If values to insert is different from columns list,
			if (col_list.Count != ((IList)values[0]).Count)
				throw new DatabaseException("Number of columns to insert is different from columns selected to insert to.");

			// Resolve all expressions in the added list.
			// For each value
			for (int i = 0; i < values.Count; ++i) {
				// Each value is a list of either expressions or "DEFAULT"
				IList insert_elements = (IList)values[i];
				int sz = insert_elements.Count;
				for (int n = 0; n < sz; ++n) {
					object elem = insert_elements[n];
					if (elem is Expression) {
						Expression exp = (Expression)elem;
						IList elem_list = exp.AllElements;
						for (int p = 0; p < elem_list.Count; ++p) {
							object ob = elem_list[p];
							if (ob is SelectStatement)
								throw new DatabaseException("Illegal to have sub-select in expression.");
						}
						// Resolve the expression.
						ResolveExpression(exp);
					}
				}
			}

			// Resolve all tables linked to this
			TableName[] linked_tables = connection.QueryTablesRelationallyLinkedTo(tableName);
			ArrayList relationallyLinkedTables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i)
				relationallyLinkedTables.Add(connection.GetTable(linked_tables[i]));


			// Check that this user has privs to insert into the table.
			if (!connection.Database.CanUserInsertIntoTableObject(context, User, tableName, col_var_list))
				throw new UserAccessException("User not permitted to insert in to table: " + tableName);

			// Are we inserting from a select statement or from a 'set' assignment
			// list?
			int insert_count = 0;

			// Set each row from the VALUES table,
			for (int i = 0; i < values.Count; ++i) {
				IList insert_elements = (IList)values[i];
				DataRow dataRow = insert_table.NewRow();
				dataRow.SetupEntire(col_index_list, insert_elements, context);
				insert_table.Add(dataRow);
				++insert_count;
			}

			// Notify TriggerManager that we've just done an update.
			if (insert_count > 0)
				connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Insert, tableName.ToString(), insert_count));

			return insert_count;
		}

		#endregion

		#region Update

		private int UpdateImpl(TableName tableName, Assignment[] assignments, Expression searchExpression, int limit) {
			// Does the table exist?
			if (!connection.TableExists(tableName))
				throw new DatabaseException("Table '" + tableName + "' does not exist.");

			// Get the table we are updating
			DataTable updateTable = connection.GetTable(tableName);

			// Form a TableSelectExpression that represents the select on the table
			TableSelectExpression select_expression = new TableSelectExpression();
			// Create the FROM clause
			select_expression.From.AddTable(tableName.ToString());
			// Set the WHERE clause
			select_expression.Where = new SearchExpression(searchExpression);

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(select_expression, connection);
			// Form the plan
			IQueryPlanNode plan = Planner.FormQueryPlan(connection, select_expression, from_set, null);

			// Resolve the variables in the assignments.
			for (int i = 0; i < assignments.Length; ++i) {
				Assignment assignment = assignments[i];
				VariableName orig_var = assignment.VariableName;
				VariableName new_var = from_set.ResolveReference(orig_var);
				if (new_var == null)
					throw new StatementException("Reference not found: " + orig_var);

				orig_var.Set(new_var);
				((IStatementTreeObject)assignment).PrepareExpressions(from_set.ExpressionQualifier);
			}

			// Resolve all tables linked to this
			TableName[] linked_tables = connection.QueryTablesRelationallyLinkedTo(tableName);
			ArrayList relationallyLinkedTables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationallyLinkedTables.Add(connection.GetTable(linked_tables[i]));
			}

			// Generate a list of Variable objects that represent the list of columns
			// being changed.
			VariableName[] col_var_list = new VariableName[assignments.Length];
			for (int i = 0; i < col_var_list.Length; ++i) {
				Assignment assign = assignments[i];
				col_var_list[i] = assign.VariableName;
			}

			// Check that this user has privs to update the table.
			if (!connection.Database.CanUserUpdateTableObject(context, connection.User, tableName, col_var_list))
				throw new UserAccessException("User not permitted to update table: " + tableName);

			// Make an array of assignments
			Assignment[] assign_list = new Assignment[assignments.Length];
			assignments.CopyTo(assign_list, 0);

			// Check the user has select permissions on the tables in the plan.
			CheckUserSelectPermissions(plan);

			// Evaluate the plan to find the update set.
			Table update_set = plan.Evaluate(context);

			// Update the data table.
			int update_count = updateTable.Update(context, update_set, assign_list, limit);

			// Notify TriggerManager that we've just done an update.
			if (update_count > 0)
				connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Update, tableName.ToString(), update_count));

			return update_count;
		}

		#endregion

		#region Transactions

		private void CommitImpl() {
			connection.Commit();
		}

		private void RollbackImpl() {
			connection.Rollback();
		}

		#endregion

		#endregion

		private void OnCommandExecuting(DirectCommand command) {
			if (CommandExecuting != null)
				CommandExecuting(this, new DirectCommandEventArgs(command.CommandType, command.ToArgsArray()));
		}

		private void OnCommandExecuted(DirectCommand command, object result) {
			if (CommandExecuted != null)
				CommandExecuted(this, new DirectCommandEventArgs(command.CommandType, command.ToArgsArray(), result));
		}

		private void OnCommandError(DirectCommand command, Exception error) {
			if (CommandError != null)
				CommandError(this, new DirectCommandEventArgs(command.CommandType, command.ToArgsArray(), error));
		}

		private delegate object CommandDelegate(DirectCommand command);

		public IAsyncResult BeginExecuteCommand(DirectCommand command, AsyncCallback callback, object state) {
			CommandDelegate del = new CommandDelegate(ExecuteCommand);
			return del.BeginInvoke(command, callback, state);
		}

		public object EndExecuteCommand(IAsyncResult result) {
			CommandDelegate del = (CommandDelegate) result.AsyncState;
			return del.EndInvoke(result);
		}

		public object ExecuteCommand(DirectCommand command) {
			LockingMechanism locker = connection.LockingMechanism;
			LockingMode lock_mode = LockingMode.None;
			object response = null;

			try {
				try {

					// For simplicity - all database locking is now exclusive inside
					// a transaction.  This means it is not possible to execute
					// queries concurrently inside a transaction.  However, we are
					// still able to execute queries concurrently from different
					// connections.
					//
					// It's debatable whether we even need to perform this Lock anymore
					// because we could change the contract of this method so that
					// it is not thread safe.  This would require that the callee ensures
					// more than one thread can not execute queries on the connection.
					lock_mode = LockingMode.Exclusive;
					locker.SetMode(lock_mode);

					OnCommandExecuting(command);

					response = ExecuteCommandImpl(command);

					OnCommandExecuted(command, response);

					// Return the result.
					return response;
				} catch (Exception e) {
					OnCommandError(command, e);
					throw;
				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lock_mode != LockingMode.None) {
							locker.FinishMode(lock_mode);
						}
					} catch(Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
						connection.Debug.Write(DebugLevel.Error, this, "Exception finishing locks");
						connection.Debug.WriteException(e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}

			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.Exclusive);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							connection.Rollback();
						} else {
							// Otherwise commit.
							connection.Commit();
						}
					} finally {
						locker.FinishMode(LockingMode.Exclusive);
					}
				}
			}
		}

		#region Private Util Methods

		private bool CheckColumnNamesMatch(string col1, String col2) {
			return String.Compare(col1, col2, connection.IsInCaseInsensitiveMode) == 0;
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

		private void CheckUserSelectPermissions(IQueryPlanNode plan) {
			// Discover the list of TableName objects this command touches,
			ArrayList touched_tables = plan.DiscoverTableNames(new ArrayList());
			Database dbase = context.Database;
			// Check that the user is allowed to select from these tables.
			for (int i = 0; i < touched_tables.Count; ++i) {
				TableName t = (TableName)touched_tables[i];
				if (!dbase.CanUserSelectFromTableObject(context, User, t, null)) {
					throw new UserAccessException("User not permitted to select from table: " + t);
				}
			}
		}

		private static bool IsIdentitySelect(TableSelectExpression expression) {
			if (expression.Columns.Count != 1)
				return false;
			if (expression.From == null)
				return false;
			if (expression.From.AllTables.Count != 1)
				return false;

			SelectColumn column = (SelectColumn)expression.Columns[0];
			if (column.resolved_name == null)
				return false;
			if (column.resolved_name.Name != "IDENTITY")
				return false;

			return true;
		}

		private void ResolveExpression(Expression exp) {
			// NOTE: This gets variables in all function parameters.
			IList vars = exp.AllVariables;
			for (int i = 0; i < vars.Count; ++i) {
				VariableName v = (VariableName)vars[i];
				VariableName to_set = ResolveColumn(v);
				v.Set(to_set);
			}
		}

		private TableName ResolveName(TableName name) {
			string schema_name = connection.CurrentSchema;
			name = TableName.Resolve(schema_name, name.Name);

			string name_strip = name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Table name can not contain '.' character.");

			return name;
		}

		private VariableName ResolveColumn(VariableName v) {
			// Try and resolve against alias names first,
			ArrayList list = new ArrayList();

			TableName tname = v.TableName;
			String sch_name = null;
			String tab_name = null;
			String col_name = v.Name;
			if (tname != null) {
				sch_name = tname.Schema;
				tab_name = tname.Name;
			}

			int matches_found = 0;
			// Find matches in our list of tables sources,
			for (int i = 0; i < table_list.Count; ++i) {
				IFromTableSource table = (IFromTableSource) table_list[i];
				int rcc = table.ResolveColumnCount(null, sch_name, tab_name, col_name);
				if (rcc == 1) {
					VariableName matched = table.ResolveColumn(null, sch_name, tab_name, col_name);
					list.Add(matched);
				} else if (rcc > 1) {
					throw new StatementException("Ambiguous column name (" + v + ")");
				}
			}

			int total_matches = list.Count;
			if (total_matches == 0)
				throw new StatementException("Can't find column: " + v);
			if (total_matches == 1)
				return (VariableName) list[0];

			if (total_matches > 1)
				// if there more than one match, check if they all match the identical
				// resource,
				throw new StatementException("Ambiguous column name (" + v + ")");

			// Should never reach here but we include this exception to keep the
			// compiler happy.
			throw new ApplicationException("Negative total matches?");
		}

		private void AddTable(IFromTableSource table) {
			table_list.Add(table);
		}

		#endregion


		#region Schemata Management

		public void CreateSchema(string name) {
			ExecuteCommand(new DirectCommand(DirectCommandType.CreateSchema, "schema", name));
		}

		public void DropSchema(string name) {
			ExecuteCommand(new DirectCommand(DirectCommandType.DropSchema, "schema", name));
		}

		#endregion

		#region Tables Management

		public void AddConstraint(TableName tableName, DataTableConstraintDef constraint) {
			AlterTableAction action = new AlterTableAction();
			action.Action = AlterTableActionType.AddConstraint;
			action.Elements.Add(constraint);
			ArrayList actions = new ArrayList();
			actions.Add(action);
			AlterTable(tableName, actions);
		}

		public void DropConstraint(TableName tableName, string constraintName) {
			AlterTableAction action = new AlterTableAction();
			action.Action = AlterTableActionType.DropConstraint;
			action.Elements.Add(constraintName);
			ArrayList actions = new ArrayList();
			actions.Add(action);
			AlterTable(tableName, actions);
		}

		public void DropPrimaryKey(TableName tableName) {
			AlterTableAction action = new AlterTableAction();
			action.Action = AlterTableActionType.DropPrimaryKey;
			ArrayList actions = new ArrayList();
			actions.Add(action);
			AlterTable(tableName, actions);
		}

		public void CreateTable(DataTableDef tableDef, bool ifNotExists) {
			ExecuteCommand(new DirectCommand(DirectCommandType.CreateTable, "table", tableDef, "if_not_exists", ifNotExists));
		}

		public void AlterCreateTable(DataTableDef tableDef) {
			ExecuteCommand(new DirectCommand(DirectCommandType.AlterCreateTable, "table", tableDef));
		}

		public void AlterTable(TableName tableName, IList actions) {
			ExecuteCommand(new DirectCommand(DirectCommandType.AlterTable, "table_name", tableName, "actions", actions));
		}

		public int DropTables(TableName[] tableNames, bool onlyIfExist) {
			return (int) ExecuteCommand(new DirectCommand(DirectCommandType.DropTables, "table_names", tableNames, "only_if_exist", onlyIfExist));
		}

		public bool DropTable(TableName tableName, bool onlyIfExists) {
			return DropTables(new TableName[] {tableName}, onlyIfExists) != 1;
		}

		#endregion

		#region Views Management

		public void CreateView(TableName name, string[] columnNames, TableSelectExpression selectExpression) {
			ExecuteCommand(new DirectCommand(DirectCommandType.CreateView, "name", name, "columns", columnNames, "select",
			                                 selectExpression));
		}

		public void CreateView(TableName name, TableSelectExpression selectExpression) {
			CreateView(name, null, selectExpression);
		}

		public void DropView(TableName name) {
			ExecuteCommand(new DirectCommand(DirectCommandType.DropView, "name", name));
		}

		#endregion

		#region Triggers Management

		public void CreateCallbackTrigger(string name, TableName tableName, TriggerEventType eventType) {
			ExecuteCommand(new DirectCommand(DirectCommandType.CreateTrigger, "name", name, "table_name", tableName, "event_type",
			                                 eventType));
		}

		public void DropCallbackTrigger(string name) {
			ExecuteCommand(new DirectCommand(DirectCommandType.DropTrigger, "name", name));
		}

		public void CreateProcedureTrigger(TableName name, TableName tableName, TriggerEventType eventType, ProcedureName procedureName, Expression[] args) {
			ExecuteCommand(new DirectCommand(DirectCommandType.CreateTrigger, "name", name, "table_name", tableName, "event_type",
			                                 eventType, "procedure", procedureName, "args", args));
		}

		public void DropProcedureTrigger(TableName name) {
			ExecuteCommand(new DirectCommand(DirectCommandType.DropTrigger, "name", name));
		}

		#endregion

		#region Procedures Management

		public TObject Call(ProcedureName procedureName, Expression[] args) {
			return (TObject) ExecuteCommand(new DirectCommand(DirectCommandType.Call, "name", procedureName, "args", args));
		}

		#endregion

		#region Select

		public Table Select(TableSelectExpression selectExpression, ByColumn[] orderBy) {
			return (Table) ExecuteCommand(new DirectCommand(DirectCommandType.Select, "expression", selectExpression, "order_by", orderBy));
		}

		public Table Select(TableSelectExpression selectExpression) {
			return Select(selectExpression, null);
		}

		#endregion

		#region Insert

		public void Insert(TableName tableName, Assignment[] assignments) {
			ExecuteCommand(new DirectCommand(DirectCommandType.Insert, "table_name", tableName, "assignments", assignments));
		}

		public int Insert(TableName tableName, IList values) {
			return Insert(tableName, new string[0], values);
		}

		public int Insert(TableName tableName, string[] columns, IList values) {
			return (int) ExecuteCommand(new DirectCommand(DirectCommandType.Insert, "table_name", tableName, "columns", columns,
			                                              "values", values));
		}

		#endregion

		#region Delete

		public int Delete(TableName tableName, Expression searchExpression, int limit) {
			return (int)ExecuteCommand(new DirectCommand(DirectCommandType.Delete, "table_name", tableName, "search_expression",
				                                 searchExpression, "limit", limit));
		}

		public int Delete(TableName tableName, Expression searchExpression) {
			return Delete(tableName, searchExpression, -1);
		}

		#endregion

		#region Update

		public int Update(TableName tableName, Assignment[] assignments, Expression searchExpression, int limit) {
			return (int)ExecuteCommand(new DirectCommand(DirectCommandType.Update, "table_name", tableName, "assignments", assignments,
				                                 "search_expression", searchExpression, "limit", limit));
		}

		public int Update(TableName tableName, Assignment[] assignments, Expression searchExpression) {
			return Update(tableName, assignments,searchExpression,  -1);
		}

		#endregion

		#region Transaction

		public void Commit() {
			ExecuteCommand(new DirectCommand(DirectCommandType.Commit));
		}

		public void Rollback() {
			ExecuteCommand(new DirectCommand(DirectCommandType.Rollback));
		}

		public void SetAutoCommit(bool state) {
			connection.AutoCommit = state;
		}

		public void SetDefaultSchema(string schema) {
			connection.SetDefaultSchema(schema);
		}

		#endregion

		#region Implementation of IDisposable

		public void Dispose() {
			connection.Dispose();
		}

		#endregion

		#region Implementation of CallBack

		void DatabaseConnection.CallBack.TriggerNotify(string trigger_name, TriggerEventType trigger_event, string trigger_source, int fire_count) {
			TriggerEventArgs args = new TriggerEventArgs(trigger_source, trigger_name, trigger_event, fire_count);
			if (TriggerEvent != null)
				TriggerEvent(this, args);
		}

		#endregion
	}
}