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

using Deveel.Data.QueryPlanning;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Logic for the <c>DELETE FROM</c> SQL statement.
	/// </summary>
	[Serializable]
	public class DeleteStatement : Statement {
		/// <summary>
		/// The name the table that we are to delete from.
		/// </summary>
		private string tableNameString;

		/// <summary>
		/// If the delete statement has a 'where' clause, then this is set here.
		/// </summary>
		/// <remarks>
		/// If it has no 'where' clause then we apply to the entire table.
		/// </remarks>
		private SearchExpression whereCondition;

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
		private DataTable updateTable;

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
		private List<DataTable> relationallyLinkedTables;

		/// <summary>
		/// The plan for the set of records we are deleting in this query.
		/// </summary>
		private IQueryPlanNode plan;

		/// <summary>
		/// Inidicates if the statement has to get the row
		/// to delete from the cursor.
		/// </summary>
		private bool fromCursor;

		/// <summary>
		/// The name of the cursor from which to delete the current row.
		/// </summary>
		private TableName cursorName;


		// ---------- Implemented from Statement ----------
		/// <inheritdoc/>
		protected override void Prepare() {

			// Get variables from the model.
			tableNameString = GetString("table_name");
			whereCondition = (SearchExpression)GetValue("where_clause");
			limit = GetInt32("limit");
			fromCursor = GetBoolean("from_cursor");
			string cursorNameString = GetString("cursor_name");

			// ---

			// Resolve the TableName object.
			tname = ResolveTableName(tableNameString);

			// Does the table exist?
			if (!Connection.TableExists(tname))
				throw new DatabaseException("Table '" + tname + "' does not exist.");

			// if this is a statement from a cursor, check it exists.
			if (fromCursor)
				cursorName = ResolveTableName(cursorNameString);

			// Get the table we are updating
			updateTable = Connection.GetTable(tname);

			if (!fromCursor) {
				// Form a TableSelectExpression that represents the select on the table
				TableSelectExpression selectExpression = new TableSelectExpression();
				// Create the FROM clause
				selectExpression.From.AddTable(tableNameString);
				// Set the WHERE clause
				selectExpression.Where = whereCondition;

				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet fromSet = Planner.GenerateFromSet(selectExpression, Connection);
				// Form the plan
				plan = Planner.FormQueryPlan(Connection, selectExpression, fromSet, null);
			}

			// Resolve all tables linked to this
			TableName[] linkedTables = Connection.QueryTablesRelationallyLinkedTo(tname);
			relationallyLinkedTables = new List<DataTable>(linkedTables.Length);
			foreach (TableName linkedTable in linkedTables) {
				relationallyLinkedTables.Add(Connection.GetTable(linkedTable));
			}
		}

		/// <inheritdoc/>
		protected override Table Evaluate() {
			// Check that this user has privs to delete from the table.
			if (!Connection.Database.CanUserDeleteFromTableObject(QueryContext, User, tname))
				throw new UserAccessException("User not permitted to delete from table: " + tableNameString);

			int deleteCount;

			if (fromCursor) {
				// This statement deletes from the current row of a cursor
				if (QueryContext.GetCursror(cursorName) == null)
					throw new DatabaseException("The cursor '" + cursorName + "' does not exist.");

				// get the cursor from which to delete the current row
				Cursor cursor = Connection.GetCursor(cursorName);

				// Delete the row from the table
				deleteCount = updateTable.DeleteCurrent(cursor);
			} else {
				// Check the user has select permissions on the tables in the plan.
				CheckUserSelectPermissions(plan);

				// Evaluates the delete statement...

				// Evaluate the plan to find the update set.
				Table deleteSet = plan.Evaluate(QueryContext);

				// Delete from the data table.
				deleteCount = updateTable.Delete(deleteSet, limit);
			}

			// Notify TriggerManager that we've just done an update.
			if (deleteCount > 0)
				Connection.OnTriggerEvent(new TriggerEventArgs(tname, TriggerEventType.Delete, deleteCount));

			// Return the number of columns we deleted.
			return FunctionTable.ResultTable(QueryContext, deleteCount);
		}
	}
}