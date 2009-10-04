//  
//  UpdateTableStatement.cs
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
		private ArrayList column_sets;

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
		/// The plan for the set of records we are updating in this query.
		/// </summary>
		private IQueryPlanNode plan;

		// ---------- Implemented from Statement ----------

		public override void Prepare() {

			table_name = (String)cmd.GetObject("table_name");
			column_sets = (ArrayList)cmd.GetObject("assignments");
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

			// Resolve the variables in the assignments.
			for (int i = 0; i < column_sets.Count; ++i) {
				Assignment assignment = (Assignment)column_sets[i];
				Variable orig_var = assignment.Variable;
				Variable new_var = from_set.ResolveReference(orig_var);
				if (new_var == null) {
					throw new StatementException("Reference not found: " + orig_var);
				}
				orig_var.Set(new_var);
				assignment.PrepareExpressions(from_set.ExpressionQualifier);
			}

			// Resolve all tables linked to this
			TableName[] linked_tables =
									 database.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(database.GetTable(linked_tables[i]));
			}

		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Generate a list of Variable objects that represent the list of columns
			// being changed.
			Variable[] col_var_list = new Variable[column_sets.Count];
			for (int i = 0; i < col_var_list.Length; ++i) {
				Assignment assign = (Assignment)column_sets[i];
				col_var_list[i] = assign.Variable;
			}

			// Check that this user has privs to update the table.
			if (!database.Database.CanUserUpdateTableObject(context,
														  user, tname, col_var_list)) {
				throw new UserAccessException(
									"User not permitted to update table: " + table_name);
			}

			// Check the user has select permissions on the tables in the plan.
			SelectStatement.CheckUserSelectPermissions(context, user, plan);

			// Evaluate the plan to find the update set.
			Table update_set = plan.Evaluate(context);

			// Make an array of assignments
			Assignment[] assign_list = (Assignment[])column_sets.ToArray(typeof(Assignment));
			// Update the data table.
			int update_count = update_table.Update(context,
												   update_set, assign_list, limit);

			// Notify TriggerManager that we've just done an update.
			if (update_count > 0) {
				database.OnTriggerEvent(new TriggerEvent(
								  TriggerEventType.Update, tname.ToString(), update_count));
			}

			// Return the number of rows we updated.
			return FunctionTable.ResultTable(context, update_count);
		}
	}
}