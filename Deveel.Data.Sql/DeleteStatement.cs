// 
//  DeleteStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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





		// ---------- Implemented from Statement ----------
		/// <inheritdoc/>
		public override void Prepare() {

			// Get variables from the model.
			table_name = (String)cmd.GetObject("table_name");
			where_condition = (SearchExpression)cmd.GetObject("where_clause");
			limit = cmd.GetInt("limit");

			// ---

			// Resolve the TableName object.
			tname = ResolveTableName(table_name, database);
			// Does the table exist?
			if (!database.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}
			// Get the table we are updating
			update_table = database.GetTable(tname);

			// Form a TableSelectExpression that represents the select on the table
			TableSelectExpression select_expression = new TableSelectExpression();
			// Create the FROM clause
			select_expression.from_clause.AddTable(table_name);
			// Set the WHERE clause
			select_expression.where_clause = where_condition;

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set =
							   Planner.GenerateFromSet(select_expression, database);
			// Form the plan
			plan = Planner.FormQueryPlan(database, select_expression, from_set, null);

			// Resolve all tables linked to this
			TableName[] linked_tables =
									 database.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(database.GetTable(linked_tables[i]));
			}

		}

		/// <inheritdoc/>
		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Check that this user has privs to delete from the table.
			if (!database.Database.CanUserDeleteFromTableObject(context,
																	 user, tname)) {
				throw new UserAccessException(
							  "User not permitted to delete from table: " + table_name);
			}

			// Check the user has select permissions on the tables in the plan.
			SelectStatement.CheckUserSelectPermissions(context, user, plan);

			// Evaluates the delete statement...

			// Evaluate the plan to find the update set.
			Table delete_set = plan.Evaluate(context);

			// Delete from the data table.
			int delete_count = update_table.Delete(delete_set, limit);

			// Notify TriggerManager that we've just done an update.
			if (delete_count > 0) {
				database.OnTriggerEvent(new TriggerEvent(
								  TriggerEventType.Delete, tname.ToString(), delete_count));
			}

			// Return the number of columns we deleted.
			return FunctionTable.ResultTable(context, delete_count);
		}
	}
}