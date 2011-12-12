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
using System.Text;

using Deveel.Data.QueryPlanning;
using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class DeclareCursorStatement : Statement {
		private CursorAttributes attributes;

		/// <summary>
		/// The name of the cursor to declare.
		/// </summary>
		private TableName resolvedName;

		private string name;

		/// <summary>
		/// The TableSelectExpression representing the select command itself.
		/// </summary>
		private TableSelectExpression selectExpression;

		/// <summary>
		/// The list of all columns to order by. (ByColumn)
		/// </summary>
		private IList<ByColumn> orderBy;

		/// <summary>
		/// The plan for evaluating this select expression.
		/// </summary>
		private IQueryPlanNode plan;

		private TableExpressionFromSet fromSet;

		protected override void Prepare() {
			DatabaseConnection db = Connection;

			name = GetString("name");

			attributes = new CursorAttributes();

			bool scrollable = GetBoolean("scrollable");
			bool update = GetBoolean("update");
			bool insensitive = GetBoolean("insensitive");

			attributes |= update ? CursorAttributes.Update : CursorAttributes.ReadOnly;

			if (update && (scrollable || insensitive))
				throw new DatabaseException("A scrollable or insensitive cursor cannot be updateable.");

			if (scrollable)
				attributes |= CursorAttributes.Scrollable;
			if (insensitive)
				attributes |= CursorAttributes.Insensitive;

			resolvedName = ResolveTableName(name);

			string nameStrip = resolvedName.Name;

			if (nameStrip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");

			// Prepare this object from the StatementTree,
			// The select expression itself
			selectExpression = (TableSelectExpression)GetValue("select_expression");
			// The order by information
			orderBy = (IList<ByColumn>) GetList("order_by");

			// Generate the TableExpressionFromSet hierarchy for the expression,
			fromSet = Planner.GenerateFromSet(selectExpression, db);

			// Form the plan
			plan = Planner.FormQueryPlan(db, selectExpression, fromSet, orderBy);
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			CheckUserSelectPermissions(plan);

			bool error = true;
			try {
				Cursor cursor = Connection.DeclareCursor(resolvedName, plan, attributes);
				cursor.From = fromSet;
				error = false;
				return FunctionTable.ResultTable(context, 0);
			} finally {
				// If an error occured, dump the command plan to the debug log.
				// Or just dump the command plan if debug level = Information
				if (Debug.IsInterestedIn(DebugLevel.Information) ||
					(error && Debug.IsInterestedIn(DebugLevel.Warning))) {
					StringBuilder buf = new StringBuilder();
					plan.DebugString(0, buf);

					Debug.Write(DebugLevel.Warning, this, "Query Plan debug:\n" + buf);
				}
			}
		}
	}
}