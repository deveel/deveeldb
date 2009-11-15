//  
//  SelectStatement.cs
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
using System.Text;

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides logics for interpreting an SQL <c>SELECT</c> statement.
	/// </summary>
	public class SelectStatement : Statement {
		/// <summary>
		/// The TableSelectExpression representing the select command itself.
		/// </summary>
		private TableSelectExpression select_expression;

		/// <summary>
		/// The list of all columns to order by. (ByColumn)
		/// </summary>
		private IList order_by;

		/// <summary>
		/// The list of columns in the 'order_by' clause fully resolved.
		/// </summary>
		private Variable[] order_cols;

		/// <summary>
		/// The plan for evaluating this select expression.
		/// </summary>
		private IQueryPlanNode plan;

		private static bool IsIdentitySelect(TableSelectExpression expression) {
			if (expression.Columns.Count != 1)
				return false;
			if (expression.From == null)
				return false;
			if (expression.From.AllTables.Count != 1)
				return false;

			SelectColumn column = (SelectColumn) expression.Columns[0];
			if (column.resolved_name == null)
				return false;
			if (column.resolved_name.Name != "IDENTITY")
				return false;

			return true;
		}



		/// <summary>
		/// Checks the permissions for this user to determine if they are 
		/// allowed to select (read) from tables in the given plan.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="plan"></param>
		/// <exception cref="UserAccessException">
		/// If the user is not allowed to select from a table in the 
		/// given plan.
		/// </exception>
		internal static void CheckUserSelectPermissions(DatabaseQueryContext context, User user, IQueryPlanNode plan) {
			// Discover the list of TableName objects this command touches,
			ArrayList touched_tables = plan.DiscoverTableNames(new ArrayList());
			Database dbase = context.Database;
			// Check that the user is allowed to select from these tables.
			for (int i = 0; i < touched_tables.Count; ++i) {
				TableName t = (TableName)touched_tables[i];
				if (!dbase.CanUserSelectFromTableObject(context, user, t, null)) {
					throw new UserAccessException("User not permitted to select from table: " + t);
				}
			}
		}

		internal override void Prepare() {
			DatabaseConnection db = Connection;

			// Prepare this object from the StatementTree,
			// The select expression itself
			select_expression = (TableSelectExpression)GetValue("table_expression");
			// The order by information
			order_by = GetList("order_by");

			// check to see if the construct is the special one for
			// selecting the latest IDENTITY value from a table
			if (IsIdentitySelect(select_expression)) {
				select_expression.Columns.RemoveAt(0);
				SelectColumn curValFunction = new SelectColumn();
				
				FromTable from_table = (FromTable) ((ArrayList) select_expression.From.AllTables)[0];
				curValFunction.SetExpression(Expression.Parse("IDENTITY('" + from_table.Name + "')"));
				curValFunction.SetAlias("IDENTITY");
				select_expression.Columns.Add(curValFunction);
			}

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(select_expression, db);

			// Form the plan
			plan = Planner.FormQueryPlan(db, select_expression, from_set, order_by);
		}


		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			CheckUserSelectPermissions(context, User, plan);

			bool error = true;
			try {
				Table t = plan.Evaluate(context);
				error = false;
				return t;
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

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[ SELECT: expression=");
			buf.Append(select_expression.ToString());
			buf.Append(" ORDER_BY=");
			buf.Append(order_by);
			buf.Append(" ]");
			return buf.ToString();
		}

	}
}