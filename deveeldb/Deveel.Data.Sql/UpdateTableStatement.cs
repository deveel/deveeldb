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
	/// The instance class that stores all the information about an 
	/// update statement for processing.
	/// </summary>
	public class UpdateTableStatement : Statement {
		/// <summary>
		/// The name the table that we are to update.
		/// </summary>
		private String table_name;

		/// <summary>
		/// An array of Assignment objects which represent what we are changing.
		/// </summary>
		private IList column_sets;

		/// <summary>
		/// If the update statement has a 'where' clause, then this is set here.
		/// </summary>
		/// <remarks>
		/// If it has no 'where' clause then we apply to the entire table.
		/// </remarks>
		SearchExpression where_condition;

		/// <summary>
		/// The limit of the number of rows that are updated by this statement.
		/// </summary>
		/// <remarks>
		/// A limit of -1 means there is no limit.
		/// </remarks>
		int limit = -1;

		/// <summary>
		/// Tables that are relationally linked to the table being inserted into, 
		/// set after <see cref="Prepare"/>.
		/// </summary>
		/// <remarks>
		/// This is used to determine the tables we need to read lock because we 
		/// need to validate relational constraints on the tables.
		/// </remarks>
		private ArrayList relationally_linked_tables;

		/// <summary>
		/// Inidicates if the statement has to get the row
		/// to delete from the cursor.
		/// </summary>
		private bool from_cursor;

		/// <summary>
		/// The name of the cursor from which to delete the current row.
		/// </summary>
		private TableName cursor_name;


		// -----

		/// <summary>
		/// The DataTable we are updating.
		/// </summary>
		private DataTable update_table;

		/// <summary>
		/// The TableName object set during 'prepare'.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// The plan for the set of records we are updating in this command.
		/// </summary>
		private IQueryPlanNode plan;

		// ---------- Implemented from Statement ----------

		protected override void Prepare() {

			table_name = GetString("table_name");
			column_sets = GetList("assignments");
			where_condition = (SearchExpression)GetValue("where_clause");
			limit = GetInteger("limit");
			from_cursor = GetBoolean("from_cursor");
			string c_name = GetString("cursor_name");

			// ---

			// Resolve the TableName object.
			tname = ResolveTableName(table_name, Connection);
			// Does the table exist?
			if (!Connection.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}

			// if this is a statement from a cursor, check it exists.
			if (from_cursor) {
				cursor_name = TableName.Resolve(Connection.CurrentSchema, c_name);
				if (!Connection.CursorExists(cursor_name))
					throw new DatabaseException("The cursor '" + c_name + "' does not exist.");
			}

			// Get the table we are updating
			update_table = Connection.GetTable(tname);

			TableExpressionFromSet from_set;

			if (!from_cursor) {
				// Form a TableSelectExpression that represents the select on the table
				TableSelectExpression select_expression = new TableSelectExpression();
				// Create the FROM clause
				select_expression.From.AddTable(table_name);
				// Set the WHERE clause
				select_expression.Where = where_condition;

				// Generate the TableExpressionFromSet hierarchy for the expression,
				from_set = Planner.GenerateFromSet(select_expression, Connection);
				// Form the plan
				plan = Planner.FormQueryPlan(Connection, select_expression, from_set, null);
			} else {
				Cursor cursor = Connection.GetCursor(cursor_name);
				if (cursor == null)
					throw new DatabaseException("The update statement cursor was not declared.");

				from_set = cursor.From;
			}

			// Resolve the variables in the assignments.
			for (int i = 0; i < column_sets.Count; ++i) {
				Assignment assignment = (Assignment)column_sets[i];
				VariableName orig_var = assignment.VariableName;
				VariableName new_var = from_set.ResolveReference(orig_var);
				if (new_var == null) {
					throw new StatementException("Reference not found: " + orig_var);
				}
				orig_var.Set(new_var);
				((IStatementTreeObject)assignment).PrepareExpressions(from_set.ExpressionQualifier);
			}

			// Resolve all tables linked to this
			TableName[] linked_tables = Connection.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(Connection.GetTable(linked_tables[i]));
			}

		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Generate a list of Variable objects that represent the list of columns
			// being changed.
			VariableName[] col_var_list = new VariableName[column_sets.Count];
			for (int i = 0; i < col_var_list.Length; ++i) {
				Assignment assign = (Assignment)column_sets[i];
				col_var_list[i] = assign.VariableName;
			}

			// Check that this user has privs to update the table.
			if (!Connection.Database.CanUserUpdateTableObject(context, User, tname, col_var_list)) {
				throw new UserAccessException( "User not permitted to update table: " + table_name);
			}

			int update_count;

			// Make an array of assignments
			Assignment[] assign_list = new Assignment[column_sets.Count];
			column_sets.CopyTo(assign_list, 0);

			if (!from_cursor) {
				// Check the user has select permissions on the tables in the plan.
				SelectStatement.CheckUserSelectPermissions(context, User, plan);

				// Evaluate the plan to find the update set.
				Table update_set = plan.Evaluate(context);

				// Update the data table.
				update_count = update_table.Update(context, update_set, assign_list, limit);
			} else {
				Cursor cursor = Connection.GetCursor(cursor_name);
				if (cursor == null)
					throw new DatabaseException("The cursor '" + cursor_name + "' was not declared.");

				update_count = update_table.UpdateCurrent(context, cursor, assign_list);
			}

			// Notify TriggerManager that we've just done an update.
			if (update_count > 0)
				Connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Update, tname.ToString(), update_count));

			// Return the number of rows we updated.
			return FunctionTable.ResultTable(context, update_count);
		}
	}
}