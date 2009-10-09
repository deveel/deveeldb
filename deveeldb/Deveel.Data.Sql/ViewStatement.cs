//  
//  ViewStatement.cs
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

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Handler for creating and dropping views in the database.
	/// </summary>
	public class ViewStatement : Statement {
		/// <summary>
		/// The type of command we are running through this ViewStatement.
		/// </summary>
		private String type;

		/// <summary>
		/// The view name to create/drop.
		/// </summary>
		private String view_name;

		/// <summary>
		/// The view name as a TableName object.
		/// </summary>
		private TableName vname;

		/// <summary>
		/// If this is a create command, the TableSelectExpression that forms 
		/// the view.
		/// </summary>
		private TableSelectExpression select_expression;

		/// <summary>
		/// If this is a create command, the IQueryPlanNode that represents the 
		/// view plan.
		/// </summary>
		private IQueryPlanNode plan;


		// ---------- Implemented from Statement ----------

		public override void Prepare() {
			type = (String)cmd.GetObject("type");
			view_name = (String)cmd.GetObject("view_name");

			String schema_name = database.CurrentSchema;
			vname = TableName.Resolve(schema_name, view_name);
			vname = database.TryResolveCase(vname);

			if (type.Equals("create")) {
				// Get the select expression
				select_expression =
							 (TableSelectExpression)cmd.GetObject("select_expression");
				// Get the column name list
				ArrayList col_list = (ArrayList)cmd.GetObject("column_list");

				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet from_set =
								   Planner.GenerateFromSet(select_expression, database);
				// Form the plan
				plan = Planner.FormQueryPlan(database, select_expression, from_set,
											 new ArrayList());

				// Wrap the result around a SubsetNode to alias the columns in the
				// table correctly for this view.
				int sz = (col_list == null) ? 0 : col_list.Count;
				Variable[] original_vars = from_set.GenerateResolvedVariableList();
				Variable[] new_column_vars = new Variable[original_vars.Length];

				if (sz > 0) {
					if (sz != original_vars.Length) {
						throw new StatementException(
							   "Column list is not the same size as the columns selected.");
					}
					for (int i = 0; i < sz; ++i) {
						String col_name = (String)col_list[i];
						new_column_vars[i] = new Variable(vname, col_name);
					}
				} else {
					sz = original_vars.Length;
					for (int i = 0; i < sz; ++i) {
						new_column_vars[i] = new Variable(vname, original_vars[i].Name);
					}
				}

				// Check there are no repeat column names in the table.
				for (int i = 0; i < sz; ++i) {
					Variable cur_v = new_column_vars[i];
					for (int n = i + 1; n < sz; ++n) {
						if (new_column_vars[n].Equals(cur_v)) {
							throw new DatabaseException(
								"Duplicate column name '" + cur_v + "' in view.  " +
								"A view may not contain duplicate column names.");
						}
					}
				}

				// Wrap the plan around a SubsetNode plan
				plan = new QueryPlan.SubsetNode(plan, original_vars, new_column_vars);

			}

		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			if (type.Equals("create")) {
				// Does the user have privs to create this tables?
				if (!database.Database.CanUserCreateTableObject(context,
																	 user, vname)) {
					throw new UserAccessException(
									  "User not permitted to create view: " + view_name);
				}

				// Does the schema exist?
				bool ignore_case = database.IsInCaseInsensitiveMode;
				SchemaDef schema =
						database.ResolveSchemaCase(vname.Schema, ignore_case);
				if (schema == null) {
					throw new DatabaseException("Schema '" + vname.Schema +
												"' doesn't exist.");
				} else {
					vname = new TableName(schema.Name, vname.Name);
				}

				// Check the permissions for this user to select from the tables in the
				// given plan.
				SelectStatement.CheckUserSelectPermissions(context, user, plan);

				// Does the table already exist?
				if (database.TableExists(vname)) {
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

				// We have to execute the plan to get the DataTableDef that represents the
				// result of the view execution.
				Table t = plan.Evaluate(context);
				DataTableDef data_table_def = new DataTableDef(t.DataTableDef);
				data_table_def.TableName = vname;

				// Create a ViewDef object,
				ViewDef view_def = new ViewDef(data_table_def, plan_copy);

				// And create the view object,
				database.CreateView(command, view_def);

				// The initial grants for a view is to give the user who created it
				// full access.
				database.GrantManager.Grant(
					 Privileges.TableAll, GrantObject.Table, vname.ToString(),
					 user.UserName, true, Database.InternalSecureUsername);

			} else if (type.Equals("drop")) {

				// Does the user have privs to drop this tables?
				if (!database.Database.CanUserDropTableObject(context,
																   user, vname)) {
					throw new UserAccessException(
									  "User not permitted to drop view: " + view_name);
				}

				// Drop the view object
				database.DropView(vname);

				// Drop the grants for this object
				database.GrantManager.RevokeAllGrantsOnObject(
												GrantObject.Table, vname.ToString());

			} else {
				throw new ApplicationException("Unknown view command type: " + type);
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}