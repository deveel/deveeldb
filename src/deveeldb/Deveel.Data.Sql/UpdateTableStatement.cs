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
using System.Collections.Generic;

using Deveel.Data.QueryPlanning;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The instance class that stores all the information about an 
	/// update statement for processing.
	/// </summary>
	public sealed class UpdateTableStatement : Statement {
		/// <summary>
		/// The name the table that we are to update.
		/// </summary>
		private string tableNameString;

		/// <summary>
		/// An array of Assignment objects which represent what we are changing.
		/// </summary>
		private IList<Assignment> columnSets;

		/// <summary>
		/// If the update statement has a 'where' clause, then this is set here.
		/// </summary>
		/// <remarks>
		/// If it has no 'where' clause then we apply to the entire table.
		/// </remarks>
		SearchExpression whereCondition;

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
		private List<Table> relationallyLinkedTables;

		/// <summary>
		/// Inidicates if the statement has to get the row
		/// to delete from the cursor.
		/// </summary>
		private bool fromCursor;

		/// <summary>
		/// The name of the cursor from which to delete the current row.
		/// </summary>
		private TableName cursorName;


		// -----

		/// <summary>
		/// The DataTable we are updating.
		/// </summary>
		private DataTable updateTable;

		/// <summary>
		/// The TableName object set during 'prepare'.
		/// </summary>
		private TableName tableName;

		/// <summary>
		/// The plan for the set of records we are updating in this command.
		/// </summary>
		private IQueryPlanNode plan;

		// ---------- Implemented from Statement ----------

		protected override void Prepare() {

			tableNameString = GetString("tableNameString");
			columnSets = (IList<Assignment>) GetList("assignments", typeof(Assignment));
			whereCondition = (SearchExpression)GetValue("where_clause");
			limit = GetInt32("limit");
			fromCursor = GetBoolean("fromCursor");
			string cursorNameString = GetString("cursor_name");

			// ---

			// Resolve the TableName object.
			tableName = ResolveTableName(tableNameString);

			// Does the table exist?
			if (!Connection.TableExists(tableName))
				throw new DatabaseException("Table '" + tableName + "' does not exist.");

			// if this is a statement from a cursor, check it exists.
			if (fromCursor) {
				cursorName = TableName.Resolve(Connection.CurrentSchema, cursorNameString);
				if (!Connection.CursorExists(cursorName))
					throw new DatabaseException("The cursor '" + cursorNameString + "' does not exist.");
			}

			// Get the table we are updating
			updateTable = Connection.GetTable(tableName);

			TableExpressionFromSet fromSet;

			if (!fromCursor) {
				// Form a TableSelectExpression that represents the select on the table
				TableSelectExpression selectExpression = new TableSelectExpression();
				// Create the FROM clause
				selectExpression.From.AddTable(tableNameString);
				// Set the WHERE clause
				selectExpression.Where = whereCondition;

				// Generate the TableExpressionFromSet hierarchy for the expression,
				fromSet = Planner.GenerateFromSet(selectExpression, Connection);
				// Form the plan
				plan = Planner.FormQueryPlan(Connection, selectExpression, fromSet, null);
			} else {
				Cursor cursor = Connection.GetCursor(cursorName);
				if (cursor == null)
					throw new DatabaseException("The update statement cursor was not declared.");

				fromSet = cursor.From;
			}

			// Resolve the variables in the assignments.
			foreach (Assignment assignment in columnSets) {
				VariableName origVar = assignment.VariableName;
				VariableName newVar = fromSet.ResolveReference(origVar);
				if (newVar == null)
					throw new StatementException("Reference not found: " + origVar);

				origVar.Set(newVar);
				((IStatementTreeObject)assignment).PrepareExpressions(fromSet.ExpressionQualifier);
			}

			// Resolve all tables linked to this
			TableName[] linkedTables = Connection.QueryTablesRelationallyLinkedTo(tableName);
			relationallyLinkedTables = new List<Table>(linkedTables.Length);
			for (int i = 0; i < linkedTables.Length; ++i) {
				relationallyLinkedTables.Add(Connection.GetTable(linkedTables[i]));
			}

		}

		protected override Table Evaluate() {
			// Generate a list of Variable objects that represent the list of columns
			// being changed.
			VariableName[] colVarList = new VariableName[columnSets.Count];
			for (int i = 0; i < colVarList.Length; ++i) {
				Assignment assign = columnSets[i];
				colVarList[i] = assign.VariableName;
			}

			// Check that this user has privs to update the table.
			if (!Connection.Database.CanUserUpdateTableObject(QueryContext, User, tableName, colVarList))
				throw new UserAccessException("User not permitted to update table: " + tableNameString);

			int updateCount;

			// Make an array of assignments
			Assignment[] assignList = new Assignment[columnSets.Count];
			columnSets.CopyTo(assignList, 0);

			if (!fromCursor) {
				// Check the user has select permissions on the tables in the plan.
				CheckUserSelectPermissions(plan);

				// Evaluate the plan to find the update set.
				Table updateSet = plan.Evaluate(QueryContext);

				// Update the data table.
				updateCount = updateTable.Update(QueryContext, updateSet, assignList, limit);
			} else {
				Cursor cursor = Connection.GetCursor(cursorName);
				if (cursor == null)
					throw new DatabaseException("The cursor '" + cursorName + "' was not declared.");

				updateCount = updateTable.UpdateCurrent(QueryContext, cursor, assignList);
			}

			// Notify TriggerManager that we've just done an update.
			if (updateCount > 0)
				Connection.OnTriggerEvent(new TriggerEventArgs(tableName, TriggerEventType.Update, updateCount));

			// Return the number of rows we updated.
			return FunctionTable.ResultTable(QueryContext, updateCount);
		}
	}
}