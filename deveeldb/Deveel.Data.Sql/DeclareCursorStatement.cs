//  
//  DeclareCursorStatement.cs
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

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	public sealed class DeclareCursorStatement : Statement {
		/// <summary>
		/// If the cursor is scrollable.
		/// </summary>
		private bool scrollable;

		/// <summary>
		/// The name of the cursor to declare.
		/// </summary>
		private TableName resolved_name;

		private string name;

		/// <summary>
		/// The TableSelectExpression representing the select command itself.
		/// </summary>
		private TableSelectExpression select_expression;

		/// <summary>
		/// The list of all columns to order by. (ByColumn)
		/// </summary>
		private IList order_by;

		/// <summary>
		/// The plan for evaluating this select expression.
		/// </summary>
		private IQueryPlanNode plan;

		private TableExpressionFromSet from_set;

		internal override void Prepare() {
			DatabaseConnection db = Connection;

			name = GetString("name");

			scrollable = GetBoolean("scrollable");

			String schema_name = db.CurrentSchema;
			resolved_name = TableName.Resolve(schema_name, name);

			string name_strip = resolved_name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");

			// Prepare this object from the StatementTree,
			// The select expression itself
			select_expression = (TableSelectExpression)GetValue("select_expression");
			// The order by information
			order_by = GetList("order_by");

			// Generate the TableExpressionFromSet hierarchy for the expression,
			from_set = Planner.GenerateFromSet(select_expression, db);

			// Form the plan
			plan = Planner.FormQueryPlan(db, select_expression, from_set, order_by);
		}

		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			SelectStatement.CheckUserSelectPermissions(context, User, plan);

			bool error = true;
			try {
				Cursor cursor = Connection.DeclareCursor(resolved_name, plan, scrollable);
				cursor.From = from_set;
				error = false;
				return FunctionTable.ResultTable(context, 0);
			} finally {
				// If an error occured, dump the command plan to the debug log.
				// Or just dump the command plan if debug level = Information
				if (Debug.IsInterestedIn(DebugLevel.Information) ||
					(error && Debug.IsInterestedIn(DebugLevel.Warning))) {
					StringBuilder buf = new StringBuilder();
					plan.DebugString(0, buf);

					Debug.Write(DebugLevel.Warning, this, "Query Plan debug:\n" + buf.ToString());
				}
			}
		}
	}
}