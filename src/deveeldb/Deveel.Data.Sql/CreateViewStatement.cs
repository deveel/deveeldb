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
	public sealed class CreateViewStatement : Statement {

		/// <summary>
		/// The view name to create/drop.
		/// </summary>
		private String view_name;

		/// <summary>
		/// The view name as a TableName object.
		/// </summary>
		private TableName vname;

		/// <summary>
		/// If this is a create Query, the TableSelectExpression that forms 
		/// the view.
		/// </summary>
		private TableSelectExpression select_expression;

		/// <summary>
		/// If this is a create Query, the IQueryPlanNode that represents the 
		/// view plan.
		/// </summary>
		private IQueryPlanNode plan;


		#region Overrides of Statement

		protected override void Prepare() {
			view_name = GetString("view_name");

			String schema_name = Connection.CurrentSchema;
			vname = TableName.Resolve(schema_name, view_name);
			vname = Connection.TryResolveCase(vname);

			// Get the select expression
			select_expression = (TableSelectExpression)GetValue("select_expression");
			// Get the column name list
			IList col_list = GetList("column_list");

			// Generate the TableExpressionFromSet hierarchy for the expression,
			TableExpressionFromSet from_set = Planner.GenerateFromSet(select_expression, Connection);
			// Form the plan
			plan = Planner.FormQueryPlan(Connection, select_expression, from_set, new List<ByColumn>());

			// Wrap the result around a SubsetNode to alias the columns in the
			// table correctly for this view.
			int sz = (col_list == null) ? 0 : col_list.Count;
			VariableName[] original_vars = from_set.GenerateResolvedVariableList();
			VariableName[] new_column_vars = new VariableName[original_vars.Length];

			if (sz > 0) {
				if (sz != original_vars.Length)
					throw new StatementException("Column list is not the same size as the columns selected.");

				for (int i = 0; i < sz; ++i) {
					String col_name = (String)col_list[i];
					new_column_vars[i] = new VariableName(vname, col_name);
				}
			} else {
				sz = original_vars.Length;
				for (int i = 0; i < sz; ++i) {
					new_column_vars[i] = new VariableName(vname, original_vars[i].Name);
				}
			}

			// Check there are no repeat column names in the table.
			for (int i = 0; i < sz; ++i) {
				VariableName cur_v = new_column_vars[i];
				for (int n = i + 1; n < sz; ++n) {
					if (new_column_vars[n].Equals(cur_v)) {
						throw new DatabaseException("Duplicate column name '" + cur_v + "' in view.  " +
						                            "A view may not contain duplicate column names.");
					}
				}
			}

			// Wrap the plan around a SubsetNode plan
			plan = new SubsetNode(plan, original_vars, new_column_vars);
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the user have privs to create this tables?
			if (!Connection.Database.CanUserCreateTableObject(context,
																 User, vname)) {
				throw new UserAccessException(
								  "User not permitted to create view: " + view_name);
			}

			// Does the schema exist?
			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema =
					Connection.ResolveSchemaCase(vname.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + vname.Schema +
											"' doesn't exist.");
			} else {
				vname = new TableName(schema.Name, vname.Name);
			}

			// Check the permissions for this user to select from the tables in the
			// given plan.
			SelectStatement.CheckUserSelectPermissions(context, User, plan);

			// Does the table already exist?
			if (Connection.TableExists(vname)) {
				throw new DatabaseException("View or table with name '" + vname +
											"' already exists.");
			}

			// Before evaluation, make a clone of the plan,
			IQueryPlanNode plan_copy;
			try {
				plan_copy = (IQueryPlanNode)plan.Clone();
			} catch (Exception e) {
				Debug.WriteException(e);
				throw new DatabaseException("Clone error: " + e.Message);
			}

			// We have to execute the plan to get the DataTableInfo that represents the
			// result of the view execution.
			Table t = plan.Evaluate(context);
			DataTableInfo dataTableInfo = t.DataTableInfo.Clone();
			dataTableInfo.TableName = vname;

			// Create a ViewDef object,
			ViewDef view_def = new ViewDef(dataTableInfo, plan_copy);

			// And create the view object,
			Connection.CreateView(Query, view_def);

			// The initial grants for a view is to give the user who created it
			// full access.
			Connection.GrantManager.Grant(
				 Privileges.TableAll, GrantObject.Table, vname.ToString(),
				 User.UserName, true, Database.InternalSecureUsername);

			return FunctionTable.ResultTable(context, 0);
		}

		#endregion
	}
}