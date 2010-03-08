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
	/// Logic for the <c>DELETE FROM</c> SQL statement.
	/// </summary>
	public class DeleteStatement : Statement {
		/// <summary>
		/// The name the table that we are to delete from.
		/// </summary>
		private String table_name;

		/// <summary>
		/// If the delete statement has a 'where' clause, then this is set here.
		/// </summary>
		/// <remarks>
		/// If it has no 'where' clause then we apply to the entire table.
		/// </remarks>
		private SearchExpression where_condition;

		/// <summary>
		/// The limit of the number of rows that are updated by this statement.
		/// </summary>
		/// <remarks>
		/// A limit of &lt; 0 means there is no limit.
		/// </remarks>
		private int limit = -1;

		// -----

		/// <summary>
		/// The DataTable we are deleting from .
		/// </summary>
		private DataTable update_table;

		/// <summary>
		/// The TableName object of the table being created.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// Tables that are relationally linked to the table being inserted 
		/// into, set after <see cref="Prepare"/>.
		/// </summary>
		/// <remarks>
		/// This is used to determine the tables we need to read lock because we 
		/// need to validate relational constraints on the tables.
		/// </remarks>
		private ArrayList relationally_linked_tables;

		/// <summary>
		/// The plan for the set of records we are deleting in this query.
		/// </summary>
		private IQueryPlanNode plan;

		/// <summary>
		/// Inidicates if the statement has to get the row
		/// to delete from the cursor.
		/// </summary>
		private bool from_cursor;

		/// <summary>
		/// The name of the cursor from which to delete the current row.
		/// </summary>
		private TableName cursor_name;


		// ---------- Implemented from Statement ----------
		/// <inheritdoc/>
		internal override void Prepare() {

			// Get variables from the model.
			table_name = GetString("table_name");
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

			if (!from_cursor) {
				// Form a TableSelectExpression that represents the select on the table
				TableSelectExpression select_expression = new TableSelectExpression();
				// Create the FROM clause
				select_expression.From.AddTable(table_name);
				// Set the WHERE clause
				select_expression.Where = where_condition;

				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet from_set = Planner.GenerateFromSet(select_expression, Connection);
				// Form the plan
				plan = Planner.FormQueryPlan(Connection, select_expression, from_set, null);
			}

			// Resolve all tables linked to this
			TableName[] linked_tables = Connection.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(Connection.GetTable(linked_tables[i]));
			}
		}

		/// <inheritdoc/>
		internal override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check that this user has privs to delete from the table.
			if (!Connection.Database.CanUserDeleteFromTableObject(context, User, tname))
				throw new UserAccessException("User not permitted to delete from table: " + table_name);

			int delete_count = 0;

			if (from_cursor) {
				// This statement deletes from the current row of a cursor

				// get the cursor from which to delete the current row
				Cursor cursor = Connection.GetCursor(cursor_name);

				// Delete the row from the table
				delete_count = update_table.DeleteCurrent(cursor);
			} else {
				// Check the user has select permissions on the tables in the plan.
				SelectStatement.CheckUserSelectPermissions(context, User, plan);

				// Evaluates the delete statement...

				// Evaluate the plan to find the update set.
				Table delete_set = plan.Evaluate(context);

				// Delete from the data table.
				delete_count = update_table.Delete(delete_set, limit);
			}

			// Notify TriggerManager that we've just done an update.
			if (delete_count > 0)
				Connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Delete, tname.ToString(), delete_count));

			// Return the number of columns we deleted.
			return FunctionTable.ResultTable(context, delete_count);
		}
	}
}