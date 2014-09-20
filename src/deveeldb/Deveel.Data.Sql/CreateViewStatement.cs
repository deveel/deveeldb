// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	public sealed class CreateViewStatement : Statement {
		/// <summary>
		/// The view name as a TableName object.
		/// </summary>
		private TableName vname;

		/// <summary>
		/// If this is a create Query, the IQueryPlanNode that represents the 
		/// view plan.
		/// </summary>
		private IQueryPlanNode plan;


		#region Overrides of Statement

		protected override void Prepare(IQueryContext context) {
			string view_name = GetString("view_name");

			vname = ResolveTableName(context, view_name);

			// Get the select expression
			TableSelectExpression selectExpression = (TableSelectExpression)GetValue("select_expression");
			// Get the column name list
			IList col_list = GetList("column_list");

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet fromSet = Planner.GenerateFromSet(selectExpression, context.Connection);

			// Form the plan
			plan = Planner.FormQueryPlan(context.Connection, selectExpression, fromSet, new List<ByColumn>());

			// Wrap the result around a SubsetNode to alias the columns in the
			// table correctly for this view.
			int sz = (col_list == null) ? 0 : col_list.Count;
			VariableName[] originalVars = fromSet.GenerateResolvedVariableList();
			VariableName[] newColumnVars = new VariableName[originalVars.Length];

			if (sz > 0 && col_list != null) {
				if (sz != originalVars.Length)
					throw new StatementException("Column list is not the same size as the columns selected.");

				for (int i = 0; i < sz; ++i) {
					String col_name = (String)col_list[i];
					newColumnVars[i] = new VariableName(vname, col_name);
				}
			} else {
				sz = originalVars.Length;
				for (int i = 0; i < sz; ++i) {
					newColumnVars[i] = new VariableName(vname, originalVars[i].Name);
				}
			}

			// Check there are no repeat column names in the table.
			for (int i = 0; i < sz; ++i) {
				VariableName cur_v = newColumnVars[i];
				for (int n = i + 1; n < sz; ++n) {
					if (newColumnVars[n].Equals(cur_v)) {
						throw new DatabaseException("Duplicate column name '" + cur_v + "' in view.  " +
						                            "A view may not contain duplicate column names.");
					}
				}
			}

			// Wrap the plan around a SubsetNode plan
			plan = new SubsetNode(plan, originalVars, newColumnVars);
		}

		protected override Table Evaluate(IQueryContext context) {
			// Does the user have privs to create this tables?
			if (!context.Connection.Database.CanUserCreateTableObject(context, vname)) {
				throw new UserAccessException( "User not permitted to create view: " + vname);
			}

			// Does the schema exist?
			SchemaDef schema = ResolveSchemaName(context, vname.Schema);
			if (schema == null)
				throw new DatabaseException("Schema '" + vname.Schema + "' doesn't exist.");

			vname = new TableName(schema.Name, vname.Name);

			// Check the permissions for this user to select from the tables in the
			// given plan.
			CheckUserSelectPermissions(context, plan);

			// Does the table already exist?
			if (context.Connection.TableExists(vname))
				throw new DatabaseException("View or table with name '" + vname + "' already exists.");

			// Before evaluation, make a clone of the plan,
			IQueryPlanNode planCopy;
			try {
				planCopy = (IQueryPlanNode)plan.Clone();
			} catch (Exception e) {
				context.Logger.Error(this, e);
				throw new DatabaseException("Clone error: " + e.Message);
			}

			// We have to execute the plan to get the DataTableInfo that represents the
			// result of the view execution.
			Table t = plan.Evaluate(context);
			DataTableInfo dataTableInfo = t.TableInfo.Clone(vname);

			// Create a View object,
			View view = new View(dataTableInfo, planCopy);

			// And create the view object,
			context.Connection.CreateView(Query, view);

			// The initial grants for a view is to give the user who created it
			// full access.
			context.Connection.GrantManager.Grant(
				 Privileges.TableAll, GrantObject.Table, vname.ToString(),
				 context.UserName, true, User.SystemName);

			return FunctionTable.ResultTable(context, 0);
		}

		#endregion
	}
}