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
using System.Text;

using Deveel.Math;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Various methods for forming command plans on SQL queries.
	/// </summary>
	internal class Planner {

		/// <summary>
		/// The name of the GROUP BY function table.
		/// </summary>
		private static TableName GROUP_BY_FUNCTION_TABLE = new TableName("FUNCTIONTABLE");


		/// <summary>
		/// Prepares the given SearchExpression object.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="from_set"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// This goes through each element of the expression. If the 
		/// element is a variable it is qualified.
		/// If the element is a <see cref="TableSelectExpression"/> it's 
		/// converted to a <see cref="SelectStatement"/> object and prepared.
		/// </remarks>
		private static void PrepareSearchExpression(
			   DatabaseConnection db, TableExpressionFromSet from_set,
			   SearchExpression expression) {
			// This is used to prepare sub-queries and qualify variables in a
			// search expression such as WHERE or HAVING.

			// Prepare the sub-queries first
			expression.Prepare(new ExpressionPreparerImpl(db, from_set));

			// Then qualify all the variables.  Note that this will not qualify
			// variables in the sub-queries.
			expression.Prepare(from_set.ExpressionQualifier);

		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			private readonly TableExpressionFromSet from_set;
			private readonly DatabaseConnection db;

			public ExpressionPreparerImpl(DatabaseConnection db, TableExpressionFromSet fromSet) {
				this.db = db;
				from_set = fromSet;
			}

			public bool CanPrepare(Object element) {
				return element is TableSelectExpression;
			}
			public Object Prepare(Object element) {
				TableSelectExpression sq_expr = (TableSelectExpression)element;
				TableExpressionFromSet sq_from_set = GenerateFromSet(sq_expr, db);
				sq_from_set.Parent = from_set;
				IQueryPlanNode sq_plan = FormQueryPlan(db, sq_expr, sq_from_set, null);
				// Form this into a command plan type
				return new TObject(TType.QueryPlanType,
								   new QueryPlan.CachePointNode(sq_plan));
			}
		}

		/// <summary>
		/// Given a <i>HAVING</i> clause expression, this will generate 
		/// a new <i>HAVING</i> clause expression with all aggregate 
		/// expressions put into the given extra function list.
		/// </summary>
		/// <param name="having_expr"></param>
		/// <param name="aggregate_list"></param>
		/// <param name="context"></param>
		/// <returns></returns>
		private static Expression FilterHavingClause(Expression having_expr,
													 ArrayList aggregate_list,
													 IQueryContext context) {
			if (having_expr.Count > 1) {
				Operator op = (Operator)having_expr.Last;
				// If logical, split and filter the left and right expressions
				Expression[] exps = having_expr.Split();
				Expression new_left =
							 FilterHavingClause(exps[0], aggregate_list, context);
				Expression new_right =
							 FilterHavingClause(exps[1], aggregate_list, context);
				Expression expr = new Expression(new_left, op, new_right);
				return expr;
			} else {
				// Not logical so determine if the expression is an aggregate or not
				if (having_expr.HasAggregateFunction(context)) {
					// Has aggregate functions so we must WriteByte this expression on the
					// aggregate list.
					aggregate_list.Add(having_expr);
					// And substitute it with a variable reference.
					VariableName v = VariableName.Resolve("FUNCTIONTABLE.HAVINGAG_" +
												  aggregate_list.Count);
					return new Expression(v);
				} else {
					// No aggregate functions so leave it as is.
					return having_expr;
				}
			}

		}

		/// <summary>
		/// Given a TableExpression, generates a TableExpressionFromSet object.
		/// </summary>
		/// <param name="select_expression"></param>
		/// <param name="db"></param>
		/// <remarks>
		/// This object is used to help qualify variable references.
		/// </remarks>
		/// <returns></returns>
		internal static TableExpressionFromSet GenerateFromSet(TableSelectExpression select_expression, DatabaseConnection db) {
			// Get the 'from_clause' from the table expression
			FromClause from_clause = select_expression.From;

			// Prepares the from_clause joining set.
			from_clause.JoinSet.Prepare(db);

			// Create a TableExpressionFromSet for this table expression
			TableExpressionFromSet from_set = new TableExpressionFromSet(db.IsInCaseInsensitiveMode);

			// Add all tables from the 'from_clause'
			IEnumerator tables = from_clause.AllTables.GetEnumerator();
			while (tables.MoveNext()) {
				FromTable ftdef = (FromTable)tables.Current;
				String unique_key = ftdef.UniqueKey;
				String alias = ftdef.Alias;

				// If this is a sub-command table,
				if (ftdef.IsSubQueryTable) {
					// eg. FROM ( SELECT id FROM Part )
					TableSelectExpression sub_query = ftdef.TableSelectExpression;
					TableExpressionFromSet sub_query_from_set = GenerateFromSet(sub_query, db);
					// The aliased name of the table
					TableName alias_table_name = null;
					if (alias != null) {
						alias_table_name = new TableName(alias);
					}
					FromTableSubQuerySource source = new FromTableSubQuerySource(db.IsInCaseInsensitiveMode, unique_key, sub_query,
					                                                             sub_query_from_set, alias_table_name);
					// Add to list of subquery tables to add to command,
					from_set.AddTable(source);
				}
					// Else must be a standard command table,
				else {
					String name = ftdef.Name;

					// Resolve to full table name
					TableName table_name = db.ResolveTableName(name);

					if (!db.TableExists(table_name))
						throw new StatementException("Table '" + table_name + "' was not found.");

					TableName given_name = null;
					if (alias != null) {
						given_name = new TableName(alias);
					}

					// Get the ITableQueryDef object for this table name (aliased).
					ITableQueryDef table_query_def = db.GetTableQueryDef(table_name, given_name);
					FromTableDirectSource source = new FromTableDirectSource(db.IsInCaseInsensitiveMode, table_query_def, unique_key,
					                                                         given_name, table_name);

					from_set.AddTable(source);
				}
			}  // while (tables.MoveNext())

			// Set up functions, aliases and exposed variables for this from set,

			// The list of columns being selected.
			List<SelectColumn> columns = select_expression.Columns;

			// For each column being selected
			for (int i = 0; i < columns.Count; ++i) {
				SelectColumn col = (SelectColumn)columns[i];
				// Is this a glob?  (eg. Part.* )
				if (col.glob_name != null) {
					// Find the columns globbed and add to the 's_col_list' result.
					if (col.glob_name.Equals("*")) {
						from_set.ExposeAllColumns();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						String tname =
									col.glob_name.Substring(0, col.glob_name.IndexOf(".*"));
						TableName tn = TableName.Resolve(tname);
						from_set.ExposeAllColumnsFromSource(tn);
					}
				} else {
					// Otherwise must be a standard column reference.  Note that at this
					// time we aren't sure if a column expression is correlated and is
					// referencing an outer source.  This means we can't verify if the
					// column expression is valid or not at this point.

					// If this column is aliased, add it as a function reference to the
					// TableExpressionFromSet.
					String alias = col.Alias;
					VariableName v = col.Expression.VariableName;
					bool alias_match_v =
							   (v != null && alias != null &&
								from_set.StringCompare(v.Name, alias));
					if (alias != null && !alias_match_v) {
						from_set.AddFunctionRef(alias, col.Expression);
						from_set.ExposeVariable(new VariableName(alias));
					} else if (v != null) {
						VariableName resolved = from_set.ResolveReference(v);
						if (resolved == null) {
							from_set.ExposeVariable(v);
						} else {
							from_set.ExposeVariable(resolved);
						}
					} else {
						String fun_name = col.Expression.Text.ToString();
						from_set.AddFunctionRef(fun_name, col.Expression);
						from_set.ExposeVariable(new VariableName(fun_name));
					}
				}

			}  // for each column selected

			return from_set;
		}

		/// <summary>
		/// Forms a command plan <see cref="IQueryPlanNode"/> from the given 
		/// <see cref="TableSelectExpression"/> and <see cref="TableExpressionFromSet"/>.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="expression">Describes the <i>SELECT</i> command 
		/// (or sub-command).</param>
		/// <param name="from_set">Used to resolve expression references.</param>
		/// <param name="order_by">A list of <see cref="ByColumn"/> objects 
		/// that represent an optional <i>ORDER BY</i> clause. If this is null 
		/// or the list is empty, no ordering is done.</param>
		/// <returns></returns>
		public static IQueryPlanNode FormQueryPlan(DatabaseConnection db,
			  TableSelectExpression expression, TableExpressionFromSet from_set,
			  IList order_by) {

			IQueryContext context = new DatabaseQueryContext(db);

			// ----- Resolve the SELECT list

			// What we are selecting
			QuerySelectColumnSet column_set = new QuerySelectColumnSet(from_set);

			// The list of columns being selected.
			List<SelectColumn> columns = expression.Columns;

			// If there are 0 columns selected, then we assume the result should
			// show all of the columns in the result.
			bool do_subset_column = (columns.Count != 0);

			// For each column being selected
			for (int i = 0; i < columns.Count; ++i) {
				SelectColumn col = columns[i];
				// Is this a glob?  (eg. Part.* )
				if (col.glob_name != null) {
					// Find the columns globbed and add to the 's_col_list' result.
					if (col.glob_name.Equals("*")) {
						column_set.SelectAllColumnsFromAllSources();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						String tname =
									col.glob_name.Substring(0, col.glob_name.IndexOf(".*"));
						TableName tn = TableName.Resolve(tname);
						column_set.SelectAllColumnsFromSource(tn);
					}
				} else {
					// Otherwise must be a standard column reference.
					column_set.SelectSingleColumn(col);
				}

			}  // for each column selected

			// Prepare the column_set,
			column_set.Prepare(context);

			// -----

			// Resolve any numerical references in the ORDER BY list (eg.
			// '1' will be a reference to column 1.

			if (order_by != null) {
				ArrayList prepared_col_set = column_set.s_col_list;
				for (int i = 0; i < order_by.Count; ++i) {
					ByColumn col = (ByColumn)order_by[i];
					Expression exp = col.Expression;
					if (exp.Count == 1) {
						Object last_elem = exp.Last;
						if (last_elem is TObject) {
							BigNumber bnum = ((TObject)last_elem).ToBigNumber();
							if (bnum.Scale == 0) {
								int col_ref = bnum.ToInt32() - 1;
								if (col_ref >= 0 && col_ref < prepared_col_set.Count) {
									SelectColumn scol = (SelectColumn)prepared_col_set[col_ref];
									col.SetExpression(new Expression(scol.Expression));
								}
							}
						}
					}
				}
			}

			// -----

			// Set up plans for each table in the from clause of the command.  For
			// sub-queries, we recurse.

			QueryTableSetPlanner table_planner = new QueryTableSetPlanner();

			for (int i = 0; i < from_set.SetCount; ++i) {
				IFromTableSource table = from_set.GetTable(i);
				if (table is FromTableSubQuerySource) {
					// This represents a sub-command in the FROM clause

					FromTableSubQuerySource sq_table = (FromTableSubQuerySource)table;
					TableSelectExpression sq_expr = sq_table.TableExpression;
					TableExpressionFromSet sq_from_set = sq_table.FromSet;

					// Form a plan for evaluating the sub-command FROM
					IQueryPlanNode sq_plan = FormQueryPlan(db, sq_expr, sq_from_set, null);

					// The top should always be a SubsetNode,
					if (sq_plan is QueryPlan.SubsetNode) {
						QueryPlan.SubsetNode subset_node = (QueryPlan.SubsetNode)sq_plan;
						subset_node.SetGivenName(sq_table.AliasedName);
					} else {
						throw new Exception("Top plan is not a SubsetNode!");
					}

					table_planner.AddTableSource(sq_plan, sq_table);
				} else if (table is FromTableDirectSource) {
					// This represents a direct referencable table in the FROM clause

					FromTableDirectSource ds_table = (FromTableDirectSource)table;
					TableName given_name = ds_table.GivenTableName;
					TableName root_name = ds_table.RootTableName;
					String aliased_name = null;
					if (!root_name.Equals(given_name)) {
						aliased_name = given_name.Name;
					}

					IQueryPlanNode ds_plan = ds_table.CreateFetchQueryPlanNode();
					table_planner.AddTableSource(ds_plan, ds_table);
				} else {
					throw new Exception(
								 "Unknown table source instance: " + table.GetType());
				}

			}

			// -----

			// The WHERE and HAVING clauses
			SearchExpression where_clause = expression.Where;
			SearchExpression having_clause = expression.Having;

			// Look at the join set and resolve the ON Expression to this statement
			JoiningSet join_set = expression.From.JoinSet;

			// Perform a quick scan and see if there are any outer joins in the
			// expression.
			bool all_inner_joins = true;
			for (int i = 0; i < join_set.TableCount - 1; ++i) {
				JoinType type = join_set.GetJoinType(i);
				if (type != JoinType.Inner) {
					all_inner_joins = false;
				}
			}

			// Prepare the joins
			for (int i = 0; i < join_set.TableCount - 1; ++i) {
				JoinType type = join_set.GetJoinType(i);
				Expression on_expression = join_set.GetOnExpression(i);

				if (all_inner_joins) {
					// If the whole join set is inner joins then simply move the on
					// expression (if there is one) to the WHERE clause.
					if (on_expression != null) {
						where_clause.AppendExpression(on_expression);
					}
				} else {
					// Not all inner joins,
					if (type == JoinType.Inner && on_expression == null) {
						// Regular join with no ON expression, so no preparation necessary
					} else {
						// Either an inner join with an ON expression, or an outer join with
						// ON expression
						if (on_expression == null) {
							throw new Exception("No ON expression in join.");
						}
						// Resolve the on_expression
						on_expression.Prepare(from_set.ExpressionQualifier);
						// And set it in the planner
						table_planner.SetJoinInfoBetweenSources(i, type, on_expression);
					}
				}

			}

			// Prepare the WHERE and HAVING clause, qualifies all variables and
			// prepares sub-queries.
			PrepareSearchExpression(db, from_set, where_clause);
			PrepareSearchExpression(db, from_set, having_clause);

			// Any extra Aggregate functions that are part of the HAVING clause that
			// we need to add.  This is a list of a name followed by the expression
			// that contains the aggregate function.
			ArrayList extra_aggregate_functions = new ArrayList();
			Expression new_having_clause = null;
			if (having_clause.FromExpression != null) {
				new_having_clause =
					  FilterHavingClause(having_clause.FromExpression,
										 extra_aggregate_functions, context);
				having_clause.SetFromExpression(new_having_clause);
			}

			// Any GROUP BY functions,
			ArrayList group_by_functions = new ArrayList();

			// Resolve the GROUP BY variable list references in this from set
			IList group_list_in = expression.GroupBy;
			int gsz = group_list_in.Count;
			VariableName[] group_by_list = new VariableName[gsz];
			for (int i = 0; i < gsz; ++i) {
				ByColumn by_column = (ByColumn)group_list_in[i];
				Expression exp = by_column.Expression;
				// Prepare the group by expression
				exp.Prepare(from_set.ExpressionQualifier);
				// Is the group by variable a complex expression?
				VariableName v = exp.VariableName;

				Expression group_by_expression;
				if (v == null) {
					group_by_expression = exp;
				} else {
					// Can we dereference the variable to an expression in the SELECT?
					group_by_expression = from_set.DereferenceAssignment(v);
				}

				if (group_by_expression != null) {
					if (group_by_expression.HasAggregateFunction(context)) {
						throw new StatementException("Aggregate expression '" +
							group_by_expression.Text.ToString() +
							"' is not allowed in GROUP BY clause.");
					}
					// Complex expression so add this to the function list.
					int group_by_fun_num = group_by_functions.Count;
					group_by_functions.Add(group_by_expression);
					v = new VariableName(GROUP_BY_FUNCTION_TABLE,
									 "#GROUPBY-" + group_by_fun_num);
				}
				group_by_list[i] = v;
			}

			// Resolve GROUP MAX variable to a reference in this from set
			VariableName groupmax_column = expression.GroupMax;
			if (groupmax_column != null) {
				VariableName v = from_set.ResolveReference(groupmax_column);
				if (v == null) {
					throw new StatementException("Could find GROUP MAX reference '" +
												 groupmax_column + "'");
				}
				groupmax_column = v;
			}

			// -----

			// Now all the variables should be resolved and correlated variables set
			// up as appropriate.

			// If nothing in the FROM clause then simply evaluate the result of the
			// select
			if (from_set.SetCount == 0) {
				if (column_set.aggregate_count > 0) {
					throw new StatementException(
						"Invalid use of aggregate function in select with no FROM clause");
				}
				// Make up the lists
				ArrayList s_col_list1 = column_set.s_col_list;
				int sz1 = s_col_list1.Count;
				String[] col_names = new String[sz1];
				Expression[] exp_list = new Expression[sz1];
				VariableName[] subset_vars = new VariableName[sz1];
				VariableName[] aliases1 = new VariableName[sz1];
				for (int i = 0; i < sz1; ++i) {
					SelectColumn scol = (SelectColumn)s_col_list1[i];
					exp_list[i] = scol.Expression;
					col_names[i] = scol.internal_name.Name;
					subset_vars[i] = new VariableName(scol.internal_name);
					aliases1[i] = new VariableName(scol.resolved_name);
				}

				return new QueryPlan.SubsetNode(
						 new QueryPlan.CreateFunctionsNode(
						   new QueryPlan.SingleRowTableNode(), exp_list, col_names),
						 subset_vars, aliases1);
			}

			// Plan the where clause.  The returned node is the plan to evaluate the
			// WHERE clause.
			IQueryPlanNode node = table_planner.PlanSearchExpression(expression.Where);

			// Make up the functions list,
			ArrayList functions_list = column_set.function_col_list;
			int fsz = functions_list.Count;
			ArrayList complete_fun_list = new ArrayList();
			for (int i = 0; i < fsz; ++i) {
				SelectColumn scol = (SelectColumn)functions_list[i];
				complete_fun_list.Add(scol.Expression);
				complete_fun_list.Add(scol.internal_name.Name);
			}
			for (int i = 0; i < extra_aggregate_functions.Count; ++i) {
				complete_fun_list.Add(extra_aggregate_functions[i]);
				complete_fun_list.Add("HAVINGAG_" + (i + 1));
			}

			int fsz2 = complete_fun_list.Count / 2;
			Expression[] def_fun_list = new Expression[fsz2];
			String[] def_fun_names = new String[fsz2];
			for (int i = 0; i < fsz2; ++i) {
				def_fun_list[i] = (Expression)complete_fun_list[i * 2];
				def_fun_names[i] = (String)complete_fun_list[(i * 2) + 1];
			}

			// If there is more than 1 aggregate function or there is a group by
			// clause, then we must add a grouping plan.
			if (column_set.aggregate_count > 0 || gsz > 0) {

				// If there is no GROUP BY clause then assume the entire result is the
				// group.
				if (gsz == 0) {
					node = new QueryPlan.GroupNode(node, groupmax_column,
												   def_fun_list, def_fun_names);
				} else {
					// Do we have any group by functions that need to be planned first?
					int gfsz = group_by_functions.Count;
					if (gfsz > 0) {
						Expression[] group_fun_list = new Expression[gfsz];
						String[] group_fun_name = new String[gfsz];
						for (int i = 0; i < gfsz; ++i) {
							group_fun_list[i] = (Expression)group_by_functions[i];
							group_fun_name[i] = "#GROUPBY-" + i;
						}
						node = new QueryPlan.CreateFunctionsNode(node,
														  group_fun_list, group_fun_name);
					}

					// Otherwise we provide the 'group_by_list' argument
					node = new QueryPlan.GroupNode(node, group_by_list, groupmax_column,
												   def_fun_list, def_fun_names);

				}

			} else {
				// Otherwise no grouping is occuring.  We simply need create a function
				// node with any functions defined in the SELECT.
				// Plan a FunctionsNode with the functions defined in the SELECT.
				if (fsz > 0) {
					node = new QueryPlan.CreateFunctionsNode(node,
															 def_fun_list, def_fun_names);
				}
			}

			// The result column list
			ArrayList s_col_list = column_set.s_col_list;
			int sz = s_col_list.Count;

			// Evaluate the having clause if necessary
			if (expression.Having.FromExpression != null) {
				// Before we evaluate the having expression we must substitute all the
				// aliased variables.
				Expression having_expr = having_clause.FromExpression;
				SubstituteAliasedVariables(having_expr, s_col_list);

				PlanTableSource source = table_planner.SingleTableSource;
				source.UpdatePlan(node);
				node = table_planner.PlanSearchExpression(having_clause);
			}

			// Do we have a composite select expression to process?
			IQueryPlanNode right_composite = null;
			if (expression.NextComposite != null) {
				TableSelectExpression composite_expr = expression.NextComposite;
				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet composite_from_set =
											 GenerateFromSet(composite_expr, db);

				// Form the right plan
				right_composite =
					FormQueryPlan(db, composite_expr, composite_from_set, null);

			}

			// Do we do a final subset column?
			VariableName[] aliases = null;
			if (do_subset_column) {
				// Make up the lists
				VariableName[] subset_vars = new VariableName[sz];
				aliases = new VariableName[sz];
				for (int i = 0; i < sz; ++i) {
					SelectColumn scol = (SelectColumn)s_col_list[i];
					subset_vars[i] = new VariableName(scol.internal_name);
					aliases[i] = new VariableName(scol.resolved_name);
				}

				// If we are distinct then add the DistinctNode here
				if (expression.Distinct) {
					node = new QueryPlan.DistinctNode(node, subset_vars);
				}

				// Process the ORDER BY?
				// Note that the ORDER BY has to occur before the subset call, but
				// after the distinct because distinct can affect the ordering of the
				// result.
				if (right_composite == null && order_by != null) {
					node = PlanForOrderBy(node, order_by, from_set, s_col_list);
				}

				// Rename the columns as specified in the SELECT
				node = new QueryPlan.SubsetNode(node, subset_vars, aliases);

			} else {
				// Process the ORDER BY?
				if (right_composite == null && order_by != null) {
					node = PlanForOrderBy(node, order_by, from_set, s_col_list);
				}
			}

			// Do we have a composite to merge in?
			if (right_composite != null) {
				// For the composite
				node = new QueryPlan.CompositeNode(node, right_composite,
							expression.CompositeFunction, expression.IsCompositeAll);
				// Final order by?
				if (order_by != null) {
					node = PlanForOrderBy(node, order_by, from_set, s_col_list);
				}
				// Ensure a final subset node
				if (!(node is QueryPlan.SubsetNode) && aliases != null) {
					node = new QueryPlan.SubsetNode(node, aliases, aliases);
				}

			}

			return node;
		}

		/// <summary>
		/// Plans an ORDER BY set.
		/// </summary>
		/// <param name="plan"></param>
		/// <param name="order_by"></param>
		/// <param name="from_set"></param>
		/// <param name="s_col_list"></param>
		/// <remarks>
		/// This is given its own function because we may want to plan 
		/// this at the end of a number of composite functions.
		/// </remarks>
		/// <returns></returns>
		public static IQueryPlanNode PlanForOrderBy(IQueryPlanNode plan,
						IList order_by, TableExpressionFromSet from_set,
						ArrayList s_col_list) {

			TableName FUNCTION_TABLE = new TableName("FUNCTIONTABLE");

			// Sort on the ORDER BY clause
			if (order_by.Count > 0) {
				int sz = order_by.Count;
				VariableName[] order_list = new VariableName[sz];
				bool[] ascending_list = new bool[sz];

				ArrayList function_orders = new ArrayList();

				for (int i = 0; i < sz; ++i) {
					ByColumn column = (ByColumn)order_by[i];
					Expression exp = column.Expression;
					ascending_list[i] = column.Ascending;
					VariableName v = exp.VariableName;
					if (v != null) {
						VariableName new_v = from_set.ResolveReference(v);
						if (new_v == null) {
							throw new StatementException(
												   "Can not resolve ORDER BY variable: " + v);
						}
						SubstituteAliasedVariable(new_v, s_col_list);

						order_list[i] = new_v;
					} else {
						// Otherwise we must be ordering by an expression such as
						// '0 - a'.

						// Resolve the expression,
						exp.Prepare(from_set.ExpressionQualifier);

						// Make sure we substitute any aliased columns in the order by
						// columns.
						SubstituteAliasedVariables(exp, s_col_list);

						// The new ordering functions are called 'FUNCTIONTABLE.#ORDER-n'
						// where n is the number of the ordering expression.
						order_list[i] =
							new VariableName(FUNCTION_TABLE, "#ORDER-" + function_orders.Count);
						function_orders.Add(exp);
					}

					//        Console.Out.WriteLine(exp);

				}

				// If there are functional orderings,
				// For this we must define a new FunctionTable with the expressions,
				// then order by those columns, and then use another SubsetNode
				// command node.
				int fsz = function_orders.Count;
				if (fsz > 0) {
					Expression[] funs = new Expression[fsz];
					String[] fnames = new String[fsz];
					for (int n = 0; n < fsz; ++n) {
						funs[n] = (Expression)function_orders[n];
						fnames[n] = "#ORDER-" + n;
					}

					if (plan is QueryPlan.SubsetNode) {
						// If the top plan is a QueryPlan.SubsetNode then we use the
						//   information from it to create a new SubsetNode that
						//   doesn't include the functional orders we have attached here.
						QueryPlan.SubsetNode top_subset_node = (QueryPlan.SubsetNode)plan;
						VariableName[] mapped_names = top_subset_node.NewColumnNames;

						// Defines the sort functions
						plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
						// Then plan the sort
						plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
						// Then plan the subset
						plan = new QueryPlan.SubsetNode(plan, mapped_names, mapped_names);
					} else {
						// Defines the sort functions
						plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
						// Plan the sort
						plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
					}

				} else {
					// No functional orders so we only need to sort by the columns
					// defined.
					plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
				}

			}

			return plan;
		}

		/// <summary>
		/// Substitutes any aliased variables in the given expression 
		/// with the function name equivalent.
		/// </summary>
		/// <param name="expression"></param>
		/// <param name="s_col_list"></param>
		/// <remarks>
		/// For example, if we have a <c>SELECT 3 + 4 Bah</c> then resolving 
		/// on variable <i>Bah</i> will be subsituted to the function column
		/// that represents the result of <i>3 + 4</i>.
		/// </remarks>
		private static void SubstituteAliasedVariables(
									 Expression expression, ArrayList s_col_list) {
			IList all_vars = expression.AllVariables;
			for (int i = 0; i < all_vars.Count; ++i) {
				VariableName v = (VariableName)all_vars[i];
				SubstituteAliasedVariable(v, s_col_list);
			}
		}

		private static void SubstituteAliasedVariable(VariableName v,
													  ArrayList s_col_list) {
			if (s_col_list != null) {
				int sz = s_col_list.Count;
				for (int n = 0; n < sz; ++n) {
					SelectColumn scol = (SelectColumn)s_col_list[n];
					if (v.Equals(scol.resolved_name)) {
						v.Set(scol.internal_name);
					}
				}
			}
		}





		// ---------- Inner classes ----------

		/// <summary>
		/// A container object for the set of SelectColumn objects selected 
		/// in a command.
		/// </summary>
		private sealed class QuerySelectColumnSet {
			/// <summary>
			/// The name of the table where functions are defined.
			/// </summary>
			private static readonly TableName FUNCTION_TABLE_NAME =
													  new TableName("FUNCTIONTABLE");

			/// <summary>
			/// The tables we are selecting from.
			/// </summary>
			private readonly TableExpressionFromSet from_set;

			/// <summary>
			/// The list of SelectColumn.
			/// </summary>
			internal readonly ArrayList s_col_list;

			/// <summary>
			/// The list of functions in this column set.
			/// </summary>
			internal readonly ArrayList function_col_list;

			/// <summary>
			/// The current number of 'FUNCTIONTABLE.' columns in the table.  This is
			/// incremented for each custom column.
			/// </summary>
			private int running_fun_number = 0;

			// The count of aggregate and constant columns included in the result set.
			// Aggregate columns are, (count(*), avg(cost_of) * 0.75, etc).  Constant
			// columns are, (9 * 4, 2, (9 * 7 / 4) + 4, etc).
			internal int aggregate_count = 0;
			int constant_count = 0;

			public QuerySelectColumnSet(TableExpressionFromSet from_set) {
				this.from_set = from_set;
				s_col_list = new ArrayList();
				function_col_list = new ArrayList();
			}

			/// <summary>
			/// Adds a single SelectColumn to the list of output columns 
			/// from the command.
			/// </summary>
			/// <param name="col"></param>
			/// <remarks>
			/// Note that at this point the the information in the given 
			/// SelectColumn may not be completely qualified.
			/// </remarks>
			internal void SelectSingleColumn(SelectColumn col) {
				s_col_list.Add(col);
			}

			/// <summary>
			/// Adds all the columns from the given IFromTableSource object.
			/// </summary>
			/// <param name="table"></param>
			void AddAllFromTable(IFromTableSource table) {
				// Select all the tables
				VariableName[] vars = table.AllColumns;
				int s_col_list_max = s_col_list.Count;
				for (int i = 0; i < vars.Length; ++i) {
					// The Variable
					VariableName v = vars[i];

					// Make up the SelectColumn
					SelectColumn ncol = new SelectColumn();
					Expression e = new Expression(v);
					e.Text.Append(v.ToString());
					ncol.SetAlias(null);
					ncol.SetExpression(e);
					ncol.resolved_name = v;
					ncol.internal_name = v;

					// Add to the list of columns selected
					SelectSingleColumn(ncol);
				}
			}

			/// <summary>
			/// Adds all column from the given table object.
			/// </summary>
			/// <param name="table_name"></param>
			/// <remarks>
			/// This is used to set up the columns that are to be viewed 
			/// as the result of the select statement.
			/// </remarks>
			internal void SelectAllColumnsFromSource(TableName table_name) {
				// Attempt to find the table in the from set.
				IFromTableSource table = from_set.FindTable(
										table_name.Schema, table_name.Name);
				if (table == null) {
					throw new StatementException(table_name.ToString() +
												 ".* is not a valid reference.");
				}

				AddAllFromTable(table);
			}

			/// <summary>
			/// Sets up this queriable with all columns from all table 
			/// sources.
			/// </summary>
			internal void SelectAllColumnsFromAllSources() {
				for (int p = 0; p < from_set.SetCount; ++p) {
					IFromTableSource table = from_set.GetTable(p);
					AddAllFromTable(table);
				}
			}

			/// <summary>
			/// Adds a new hidden function into the column set.
			/// </summary>
			/// <param name="fun_alias"></param>
			/// <param name="function"></param>
			/// <param name="context"></param>
			/// <remarks>
			/// This is intended to be used for implied functions.  
			/// For example, a command may have a function in a <i>GROUP BY</i> 
			/// clause. It's desirable to include the function in the 
			/// column set but not in the final result.
			/// </remarks>
			/// <returns>
			/// Returns an absolute Variable object that can be used to 
			/// reference this hidden column.
			/// </returns>
			internal VariableName AddHiddenFunction(String fun_alias, Expression function,
									   IQueryContext context) {
				SelectColumn scol = new SelectColumn();
				scol.resolved_name = new VariableName(fun_alias);
				scol.SetAlias(fun_alias);
				scol.SetExpression(function);
				scol.internal_name = new VariableName(FUNCTION_TABLE_NAME, fun_alias);

				// If this is an aggregate column then add to aggregate count.
				if (function.HasAggregateFunction(context)) {
					++aggregate_count;
				}
					// If this is a constant column then add to constant cound.
				else if (function.IsConstant) {
					++constant_count;
				}

				function_col_list.Add(scol);

				return scol.internal_name;
			}

			/// <summary>
			/// Prepares the given SelectColumn by fully qualifying the expression and
			/// allocating it correctly within this context.
			/// </summary>
			/// <param name="col"></param>
			/// <param name="context"></param>
			private void PrepareSelectColumn(SelectColumn col, IQueryContext context) {
				// Check to see if we have any Select statements in the
				//   Expression.  This is necessary, because we can't have a
				//   sub-select evaluated during list table downloading.
				IList exp_elements = col.Expression.AllElements;
				for (int n = 0; n < exp_elements.Count; ++n) {
					if (exp_elements[n] is TableSelectExpression) {
						throw new StatementException(
												 "Sub-command not allowed in column list.");
					}
				}

				// First fully qualify the select expression
				col.Expression.Prepare(from_set.ExpressionQualifier);

				// If the expression isn't a simple variable, then add to
				// function list.
				VariableName v = col.Expression.VariableName;
				if (v == null) {
					// This means we have a complex expression.

					++running_fun_number;
					String agg_str = running_fun_number.ToString();

					// If this is an aggregate column then add to aggregate count.
					if (col.Expression.HasAggregateFunction(context)) {
						++aggregate_count;
						// Add '_A' code to end of internal name of column to signify this is
						// an aggregate column
						agg_str += "_A";
					}
						// If this is a constant column then add to constant cound.
					else if (col.Expression.IsConstant) {
						++constant_count;
					} else {
						// Must be an expression with variable's embedded ( eg.
						//   (part_id + 3) * 4, (id * value_of_part), etc )
					}
					function_col_list.Add(col);

					col.internal_name = new VariableName(FUNCTION_TABLE_NAME, agg_str);
					if (col.Alias == null) {
						col.SetAlias(col.Expression.Text.ToString());
					}
					col.resolved_name = new VariableName(col.Alias);

				} else {
					// Not a complex expression
					col.internal_name = v;
					if (col.Alias == null) {
						col.resolved_name = v;
					} else {
						col.resolved_name = new VariableName(col.Alias);
					}
				}

			}


			/// <summary>
			/// Resolves all variable objects in each column.
			/// </summary>
			/// <param name="context"></param>
			internal void Prepare(IQueryContext context) {
				// Prepare each of the columns selected.
				// NOTE: A side-effect of this is that it qualifies all the Expressions
				//   that are functions in TableExpressionFromSet.  After this section,
				//   we can dereference variables for their function Expression.
				for (int i = 0; i < s_col_list.Count; ++i) {
					SelectColumn column = (SelectColumn)s_col_list[i];
					PrepareSelectColumn(column, context);
				}
			}
		}


		/// <summary>
		/// A table set planner that maintains a list of table dependence lists and
		/// progressively constructs a plan tree from the bottom up.
		/// </summary>
		private sealed class QueryTableSetPlanner {

			/// <summary>
			/// The list of <see cref="PlanTableSource"/> objects for each source being planned.
			/// </summary>
			private ArrayList table_list;

			/// <summary>
			/// If a join has occurred since the planner was constructed or copied then
			/// this is set to true.
			/// </summary>
			private bool has_join_occurred;


			public QueryTableSetPlanner() {
				this.table_list = new ArrayList();
				has_join_occurred = false;
			}

			/// <summary>
			/// Add a <see cref="PlanTableSource"/> to this planner.
			/// </summary>
			/// <param name="source"></param>
			private void AddPlanTableSource(PlanTableSource source) {
				table_list.Add(source);
				has_join_occurred = true;
			}

			/// <summary>
			/// Returns true if a join has occurred ('table_list' has been modified).
			/// </summary>
			public bool HasJoinOccured {
				get { return has_join_occurred; }
			}

			/// <summary>
			/// Adds a new table source to the planner given a Plan that 'creates'
			/// the source.
			/// </summary>
			/// <param name="plan"></param>
			/// <param name="from_def">The <see cref="IFromTableSource"/> that describes the source 
			/// created by the plan.</param>
			public void AddTableSource(IQueryPlanNode plan, IFromTableSource from_def) {
				VariableName[] all_cols = from_def.AllColumns;
				String[] unique_names = new String[] { from_def.UniqueName };
				AddPlanTableSource(new PlanTableSource(plan, all_cols, unique_names));
			}

			/// <summary>
			/// Returns the index of the given <see cref="PlanTableSource"/> in the 
			/// table list.
			/// </summary>
			/// <param name="source"></param>
			/// <returns></returns>
			private int IndexOfPlanTableSource(PlanTableSource source) {
				int sz = table_list.Count;
				for (int i = 0; i < sz; ++i) {
					if (table_list[i] == source) {
						return i;
					}
				}
				return -1;
			}

			/// <summary>
			/// Links the last added table source to the previous added table source
			/// through this joining information.
			/// </summary>
			/// <param name="between_index">Represents the point in between the table sources that 
			/// the join should be setup for.</param>
			/// <param name="join_type"></param>
			/// <param name="on_expr"></param>
			/// <remarks>
			/// For example, to set the join between TableSource 0 and 1, use 0 as the between index. 
			/// A between index of 3 represents the join between TableSource index 2 and 2.
			/// </remarks>
			public void SetJoinInfoBetweenSources(int between_index, JoinType join_type, Expression on_expr) {
				PlanTableSource plan_left =
								  (PlanTableSource)table_list[between_index];
				PlanTableSource plan_right =
								  (PlanTableSource)table_list[between_index + 1];
				plan_left.SetRightJoinInfo(plan_right, join_type, on_expr);
				plan_right.SetLeftJoinInfo(plan_left, join_type, on_expr);
			}

			/// <summary>
			/// Forms a new PlanTableSource that's the concatination of the given two 
			/// <see cref="PlanTableSource"/> objects.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <param name="plan"></param>
			/// <returns></returns>
			public static PlanTableSource ConcatTableSources(PlanTableSource left, PlanTableSource right, IQueryPlanNode plan) {
				// Merge the variable list
				VariableName[] new_var_list = new VariableName[left.var_list.Length +
													   right.var_list.Length];
				int i = 0;
				for (int n = 0; n < left.var_list.Length; ++n) {
					new_var_list[i] = left.var_list[n];
					++i;
				}
				for (int n = 0; n < right.var_list.Length; ++n) {
					new_var_list[i] = right.var_list[n];
					++i;
				}

				// Merge the unique table names list
				String[] new_unique_list = new String[left.unique_names.Length +
													  right.unique_names.Length];
				i = 0;
				for (int n = 0; n < left.unique_names.Length; ++n) {
					new_unique_list[i] = left.unique_names[n];
					++i;
				}
				for (int n = 0; n < right.unique_names.Length; ++n) {
					new_unique_list[i] = right.unique_names[n];
					++i;
				}

				// Return the new table source plan.
				return new PlanTableSource(plan, new_var_list, new_unique_list);
			}

			/// <summary>
			/// Joins two tables when a plan is generated for joining the two tables.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <param name="merge_plan"></param>
			/// <returns></returns>
			public PlanTableSource MergeTables(PlanTableSource left, PlanTableSource right, IQueryPlanNode merge_plan) {
				// Remove the sources from the table list.
				table_list.Remove(left);
				table_list.Remove(right);
				// Add the concatination of the left and right tables.
				PlanTableSource c_plan = ConcatTableSources(left, right, merge_plan);
				c_plan.SetJoinInfoMergedBetween(left, right);
				c_plan.SetUpdated();
				AddPlanTableSource(c_plan);
				// Return the name plan
				return c_plan;
			}

			/// <summary>
			/// Finds and returns the PlanTableSource in the list of tables that
			/// contains the given <see cref="VariableName"/> reference.
			/// </summary>
			/// <param name="reference"></param>
			/// <returns></returns>
			public PlanTableSource FindTableSource(VariableName reference) {
				int sz = table_list.Count;

				// If there is only 1 plan then assume the variable is in there.
				if (sz == 1) {
					return (PlanTableSource)table_list[0];
				}

				for (int i = 0; i < sz; ++i) {
					PlanTableSource source = (PlanTableSource)table_list[i];
					if (source.ContainsVariable(reference)) {
						return source;
					}
				}
				throw new Exception(
							 "Unable to find table with variable reference: " + reference);
			}

			/// <summary>
			/// Finds a common <see cref="PlanTableSource"/> that contains the list of variables given.
			/// </summary>
			/// <param name="var_list"></param>
			/// <remarks>
			/// If the list is 0 or there is no common source then null is returned.
			/// </remarks>
			/// <returns></returns>
			public PlanTableSource FindCommonTableSource(IList var_list) {
				if (var_list.Count == 0) {
					return null;
				}

				PlanTableSource plan = FindTableSource((VariableName)var_list[0]);
				int i = 1;
				int sz = var_list.Count;
				while (i < sz) {
					PlanTableSource p2 = FindTableSource((VariableName)var_list[i]);
					if (plan != p2) {
						return null;
					}
					++i;
				}

				return plan;
			}

			/// <summary>
			/// Finds and returns the <see cref="PlanTableSource"/> in the list of table that
			/// contains the given unique key name.
			/// </summary>
			/// <param name="key"></param>
			/// <returns></returns>
			public PlanTableSource FindTableSourceWithUniqueKey(String key) {
				int sz = table_list.Count;
				for (int i = 0; i < sz; ++i) {
					PlanTableSource source = (PlanTableSource)table_list[i];
					if (source.ContainsUniqueKey(key)) {
						return source;
					}
				}
				throw new Exception(
									"Unable to find table with unique key: " + key);
			}

			/// <summary>
			/// Returns the single <see cref="PlanTableSource"/> for this planner.
			/// </summary>
			internal PlanTableSource SingleTableSource {
				get {
					if (table_list.Count != 1) {
						throw new Exception("Not a single table source.");
					}
					return (PlanTableSource) table_list[0];
				}
			}

			/// <summary>
			/// Sets a <see cref="QueryPlan.CachePointNode"/> with the given key on 
			/// all of the plan table sources in <see cref="table_list"/>.
			/// </summary>
			/// <remarks>
			/// Note that this does not change the <i>update</i> status of the table sources. 
			/// If there is currently a <see cref="QueryPlan.CachePointNode"/> on any of the 
			/// sources then no update is made.
			/// </remarks>
			private void SetCachePoints() {
				int sz = table_list.Count;
				for (int i = 0; i < sz; ++i) {
					PlanTableSource plan = (PlanTableSource)table_list[i];
					if (!(plan.Plan is QueryPlan.CachePointNode)) {
						plan.plan = new QueryPlan.CachePointNode(plan.Plan);
					}
				}
			}

			/// <summary>
			/// Creates a single <see cref="PlanTableSource"/> that encapsulates all 
			/// the given variables in a single table.
			/// </summary>
			/// <param name="all_vars"></param>
			/// <remarks>
			/// If this means a table must be joined with another using the natural join 
			/// conditions then this happens here.
			/// <para>
			/// The intention of this function is to produce a plan that encapsulates
			/// all the variables needed to perform a specific evaluation.
			/// </para>
			/// <para>
			/// This has the potential to cause 'natural join' situations which are bad 
			/// performance.  It is a good idea to perform joins using other methods before 
			/// this is used.
			/// </para>
			/// <para>
			/// This will change the 'table_list' variable in this class if tables are joined.
			/// </para>
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource JoinAllPlansWithVariables(IList all_vars) {
				// Collect all the plans that encapsulate these variables.
				ArrayList touched_plans = new ArrayList();
				int sz = all_vars.Count;
				for (int i = 0; i < sz; ++i) {
					VariableName v = (VariableName)all_vars[i];
					PlanTableSource plan = FindTableSource(v);
					if (!touched_plans.Contains(plan)) {
						touched_plans.Add(plan);
					}
				}
				// Now 'touched_plans' contains a list of PlanTableSource for each
				// plan to be joined.

				return JoinAllPlansToSingleSource(touched_plans);
			}

			/// <summary>
			/// Returns true if it is possible to naturally join the two plans.
			/// </summary>
			/// <param name="plan1"></param>
			/// <param name="plan2"></param>
			/// <remarks>
			/// Two plans can be joined under the following sitations:
			/// <list type="number">
			///		<item>The left or right plan of the first source points 
			///		to the second source.</item>
			///		<item>Either one has no left plan and the other has no 
			///		right plan, or one has no right plan and the other has 
			///		no left plan.</item>
			/// </list>
			/// </remarks>
			/// <returns></returns>
			private static int CanPlansBeNaturallyJoined(PlanTableSource plan1, PlanTableSource plan2) {
				if (plan1.left_plan == plan2 || plan1.right_plan == plan2) {
					return 0;
				} else if (plan1.left_plan != null && plan2.left_plan != null) {
					// This is a left clash
					return 2;
				} else if (plan1.right_plan != null && plan2.right_plan != null) {
					// This is a right clash
					return 1;
				} else if ((plan1.left_plan == null && plan2.right_plan == null) ||
						   (plan1.right_plan == null && plan2.left_plan == null)) {
					// This means a merge between the plans is fine
					return 0;
				} else {
					// Must be a left and right clash
					return 2;
				}
			}

			/// <summary>
			/// Given a list of <see cref="PlanTableSource"/> objects, this will produce 
			/// a plan that naturally joins all the tables together into a single plan.
			/// </summary>
			/// <param name="all_plans"></param>
			/// <remarks>
			/// The join algorithm used is determined by the information in the FROM clause. 
			/// An OUTER JOIN, for example, will join depending on the conditions provided 
			/// in the ON clause.  If no explicit join method is provided then a natural join 
			/// will be planned.
			/// <para>
			/// Care should be taken with this because this method can produce natural joins 
			/// which are often optimized out by more appropriate join expressions that can 
			/// be processed before this is called.
			/// </para>
			/// <para>
			/// This will change the <see cref="table_list"/> variable in this class if tables 
			/// are joined.
			/// </para>
			/// </remarks>
			/// <returns>
			/// Returns null if no plans are provided.
			/// </returns>
			private PlanTableSource JoinAllPlansToSingleSource(IList all_plans) {
				// If there are no plans then return null
				if (all_plans.Count == 0) {
					return null;
				}
					// Return early if there is only 1 table.
				else if (all_plans.Count == 1) {
					return (PlanTableSource)all_plans[0];
				}

				// Make a working copy of the plan list.
				ArrayList working_plan_list = new ArrayList(all_plans.Count);
				for (int i = 0; i < all_plans.Count; ++i) {
					working_plan_list.Add(all_plans[i]);
				}

				// We go through each plan in turn.
				while (working_plan_list.Count > 1) {
					PlanTableSource left_plan = (PlanTableSource)working_plan_list[0];
					PlanTableSource right_plan =
												(PlanTableSource)working_plan_list[1];
					// First we need to determine if the left and right plan can be
					// naturally joined.
					int status = CanPlansBeNaturallyJoined(left_plan, right_plan);
					if (status == 0) {
						// Yes they can so join them
						PlanTableSource new_plan = NaturallyJoinPlans(left_plan, right_plan);
						// Remove the left and right plan from the list and add the new plan
						working_plan_list.Remove(left_plan);
						working_plan_list.Remove(right_plan);
						working_plan_list.Insert(0, new_plan);
					} else if (status == 1) {
						// No we can't because of a right join clash, so we join the left
						// plan right in hopes of resolving the clash.
						PlanTableSource new_plan =
										NaturallyJoinPlans(left_plan, left_plan.right_plan);
						working_plan_list.Remove(left_plan);
						working_plan_list.Remove(left_plan.right_plan);
						working_plan_list.Insert(0, new_plan);
					} else if (status == 2) {
						// No we can't because of a left join clash, so we join the left
						// plan left in hopes of resolving the clash.
						PlanTableSource new_plan =
										 NaturallyJoinPlans(left_plan, left_plan.left_plan);
						working_plan_list.Remove(left_plan);
						working_plan_list.Remove(left_plan.left_plan);
						working_plan_list.Insert(0, new_plan);
					} else {
						throw new Exception("Unknown status: " + status);
					}
				}

				// Return the working plan of the merged tables.
				return (PlanTableSource)working_plan_list[0];

			}

			/// <summary>
			/// Naturally joins two <see cref="PlanTableSource"/> objects in this planner.
			/// </summary>
			/// <param name="plan1"></param>
			/// <param name="plan2"></param>
			/// <remarks>
			/// When this method returns the actual plans will be joined together. This method 
			/// modifies <see cref="table_list"/>.
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource NaturallyJoinPlans(PlanTableSource plan1, PlanTableSource plan2) {
				JoinType join_type;
				Expression on_expr;
				PlanTableSource left_plan, right_plan;
				// Are the plans linked by common join information?
				if (plan1.right_plan == plan2) {
					join_type = plan1.right_join_type;
					on_expr = plan1.right_on_expr;
					left_plan = plan1;
					right_plan = plan2;
				} else if (plan1.left_plan == plan2) {
					join_type = plan1.left_join_type;
					on_expr = plan1.left_on_expr;
					left_plan = plan2;
					right_plan = plan1;
				} else {
					// Assertion - make sure no join clashes!
					if ((plan1.left_plan != null && plan2.left_plan != null) ||
						(plan1.right_plan != null && plan2.right_plan != null)) {
						throw new Exception(
						   "Assertion failed - plans can not be naturally join because " +
						   "the left/right join plans clash.");
					}

					// Else we must assume a non-dependant join (not an outer join).
					// Perform a natural join
					IQueryPlanNode node1 = new QueryPlan.NaturalJoinNode(
													   plan1.Plan, plan2.Plan);
					return MergeTables(plan1, plan2, node1);
				}

				// This means plan1 and plan2 are linked by a common join and ON
				// expression which we evaluate now.
				bool outer_join;
				if (join_type == JoinType.Left) {
					// Mark the left plan
					left_plan.UpdatePlan(new QueryPlan.MarkerNode(
													left_plan.Plan, "OUTER_JOIN"));
					outer_join = true;
				} else if (join_type == JoinType.Right) {
					// Mark the right plan
					right_plan.UpdatePlan(new QueryPlan.MarkerNode(
													right_plan.Plan, "OUTER_JOIN"));
					outer_join = true;
				} else if (join_type == JoinType.Inner) {
					// Inner join with ON expression
					outer_join = false;
				} else {
					throw new Exception("Join type (" + join_type + ") is not supported.");
				}

				// Make a Planner object for joining these plans.
				QueryTableSetPlanner planner = new QueryTableSetPlanner();
				planner.AddPlanTableSource(left_plan.Copy());
				planner.AddPlanTableSource(right_plan.Copy());

				//      planner.printDebugInfo();

				// Evaluate the on expression
				IQueryPlanNode node = planner.LogicalEvaluate(on_expr);
				// If outer join add the left outer join node
				if (outer_join) {
					node = new QueryPlan.LeftOuterJoinNode(node, "OUTER_JOIN");
				}
				// And merge the plans in this set with the new node.
				return MergeTables(plan1, plan2, node);

				//      Console.Out.WriteLine("OUTER JOIN: " + on_expr);
				//      throw new RuntimeException("PENDING");

			}

			/// <summary>
			/// Plans all outer joins.
			/// </summary>
			/// <remarks>
			/// This will change the <see cref="table_list"/> variable in this class if 
			/// tables are joined.
			/// </remarks>
			private void PlanAllOuterJoins() {
				int sz = table_list.Count;
				if (sz <= 1) {
					return;
				}

				// Make a working copy of the plan list.
				ArrayList working_plan_list = new ArrayList(sz);
				for (int i = 0; i < sz; ++i) {
					working_plan_list.Add(table_list[i]);
				}

				//      Console.Out.WriteLine("----");

				PlanTableSource plan1 = (PlanTableSource)working_plan_list[0];
				for (int i = 1; i < sz; ++i) {
					PlanTableSource plan2 = (PlanTableSource)working_plan_list[i];

					//        Console.Out.WriteLine("Joining: " + plan1);
					//        Console.Out.WriteLine("   with: " + plan2);

					if (plan1.right_plan == plan2) {
						plan1 = NaturallyJoinPlans(plan1, plan2);
					} else {
						plan1 = plan2;
					}
				}

			}

			/// <summary>
			/// Naturally joins all remaining tables sources to make a final single
			/// plan which is returned.
			/// </summary>
			/// <remarks>
			/// This will change the <see cref="table_list"/> variable in this class if 
			/// tables are joined.
			/// </remarks>
			/// <returns></returns>
			private PlanTableSource NaturalJoinAll() {
				int sz = table_list.Count;
				if (sz == 1) {
					return (PlanTableSource)table_list[0];
				}

				// Produce a plan that naturally joins all tables.
				return JoinAllPlansToSingleSource(table_list);
			}

			/// <summary>
			/// Convenience class that stores an expression to evaluate for a table.
			/// </summary>
			private sealed class SingleVarPlan {
				internal PlanTableSource table_source;
				internal VariableName single_var;
				internal VariableName variable;
				internal Expression expression;
			}

			/// <summary>
			/// Adds a single var plan to the given list.
			/// </summary>
			/// <param name="list"></param>
			/// <param name="table"></param>
			/// <param name="variable"></param>
			/// <param name="single_var"></param>
			/// <param name="exp_parts"></param>
			/// <param name="op"></param>
			private static void AddSingleVarPlanTo(ArrayList list, PlanTableSource table, VariableName variable, VariableName single_var, Expression[] exp_parts, Operator op) {
				Expression exp = new Expression(exp_parts[0], op, exp_parts[1]);
				// Is this source in the list already?
				int sz = list.Count;
				for (int i = 0; i < sz; ++i) {
					SingleVarPlan plan1 = (SingleVarPlan)list[i];
					if (plan1.table_source == table &&
						(variable == null || plan1.variable.Equals(variable))) {
						// Append to end of current expression
						plan1.variable = variable;
						plan1.expression = new Expression(plan1.expression,
														 Operator.Get("and"), exp);
						return;
					}
				}
				// Didn't find so make a new entry in the list.
				SingleVarPlan plan = new SingleVarPlan();
				plan.table_source = table;
				plan.variable = variable;
				plan.single_var = single_var;
				plan.expression = exp;
				list.Add(plan);
				return;
			}

			// ----

			// An expression plan for a constant expression.  These are very
			// optimizable indeed.
			private class ConstantExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public ConstantExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Each currently open branch must have this constant expression added
					// to it.
					for (int n = 0; n < qtsp.table_list.Count; ++n) {
						PlanTableSource plan = (PlanTableSource)qtsp.table_list[n];
						plan.UpdatePlan(new QueryPlan.ConstantSelectNode(
															  plan.Plan, expression));
					}
				}
			}

			private class SimpleSelectExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName single_var;
				private readonly Operator op;
				private readonly Expression expression;

				public SimpleSelectExpressionPlan(QueryTableSetPlanner qtsp, VariableName v, Operator op,
												  Expression e) {
					this.qtsp = qtsp;
					single_var = v;
					this.op = op;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource table_source = qtsp.FindTableSource(single_var);
					table_source.UpdatePlan(new QueryPlan.SimpleSelectNode(
							   table_source.Plan, single_var, op, expression));
				}
			}

			private class SimpleSingleExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName single_var;
				private readonly Expression expression;
				public SimpleSingleExpressionPlan(QueryTableSetPlanner qtsp, VariableName v, Expression e) {
					this.qtsp = qtsp;
					single_var = v;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource table_source = qtsp.FindTableSource(single_var);
					table_source.UpdatePlan(new QueryPlan.RangeSelectNode(
													table_source.Plan, expression));
				}
			}

			private class ComplexSingleExpressionPlan : ExpressionPlan {
				private readonly QueryTableSetPlanner qtsp;
				private readonly VariableName single_var;
				private readonly Expression expression;
				public ComplexSingleExpressionPlan(QueryTableSetPlanner qtsp, VariableName v, Expression e) {
					this.qtsp = qtsp;
					single_var = v;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource table_source = qtsp.FindTableSource(single_var);
					table_source.UpdatePlan(new QueryPlan.ExhaustiveSelectNode(
													table_source.Plan, expression));
				}
			}

			private class SimplePatternExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private VariableName single_var;
				private Expression expression;
				public SimplePatternExpressionPlan(QueryTableSetPlanner qtsp, VariableName v, Expression e) {
					this.qtsp = qtsp;
					single_var = v;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Find the table source for this variable
					PlanTableSource table_source = qtsp.FindTableSource(single_var);
					table_source.UpdatePlan(new QueryPlan.SimplePatternSelectNode(
													table_source.Plan, expression));
				}
			}

			private class ExhaustiveSelectExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public ExhaustiveSelectExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					expression = e;
				}
				public override void AddToPlanTree() {
					// Get all the variables of this expression.
					IList all_vars = expression.AllVariables;
					// Find the table source for this set of variables.
					PlanTableSource table_source = qtsp.JoinAllPlansWithVariables(all_vars);
					// Perform the exhaustive select
					table_source.UpdatePlan(new QueryPlan.ExhaustiveSelectNode(
													table_source.Plan, expression));
				}
			}

			private class ExhaustiveSubQueryExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private IList all_vars;
				private Expression expression;
				public ExhaustiveSubQueryExpressionPlan(QueryTableSetPlanner qtsp, IList vars, Expression e) {
					this.qtsp = qtsp;
					this.all_vars = vars;
					this.expression = e;
				}
				public override void AddToPlanTree() {
					PlanTableSource table_source = qtsp.JoinAllPlansWithVariables(all_vars);
					// Update the plan
					table_source.UpdatePlan(new QueryPlan.ExhaustiveSelectNode(
												   table_source.Plan, expression));

				}
			}

			private class SimpleSubQueryExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public SimpleSubQueryExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					this.expression = e;
				}
				public override void AddToPlanTree() {
					Operator op = (Operator)expression.Last;
					Expression[] exps = expression.Split();
					VariableName left_var = exps[0].VariableName;
					IQueryPlanNode right_plan = exps[1].QueryPlanNode;

					// Find the table source for this variable
					PlanTableSource table_source = qtsp.FindTableSource(left_var);
					// The left branch
					IQueryPlanNode left_plan = table_source.Plan;
					// Update the plan
					table_source.UpdatePlan(
						 new QueryPlan.NonCorrelatedAnyAllNode(
							   left_plan, right_plan, new VariableName[] { left_var }, op));
				}
			}

			private class ExhaustiveJoinExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public ExhaustiveJoinExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.expression = e;
				}
				public override void AddToPlanTree() {
					// Get all the variables in the expression
					IList all_vars = expression.AllVariables;
					// Merge it into one plan (possibly performing natural joins).
					PlanTableSource all_plan = qtsp.JoinAllPlansWithVariables(all_vars);
					// And perform the exhaustive select,
					all_plan.UpdatePlan(new QueryPlan.ExhaustiveSelectNode(
													   all_plan.Plan, expression));
				}
			}

			private class StandardJoinExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public StandardJoinExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					this.expression = e;
				}
				public override void AddToPlanTree() {

					// Get the expression with the multiple variables
					Expression[] exps = expression.Split();

					// Get the list of variables in the left hand and right hand side
					VariableName lhs_v = exps[0].VariableName;
					VariableName rhs_v = exps[1].VariableName;
					IList lhs_vars = exps[0].AllVariables;
					IList rhs_vars = exps[1].AllVariables;

					// Get the operator
					Operator op = (Operator)expression.Last;

					// Get the left and right plan for the variables in the expression.
					// Note that these methods may perform natural joins on the table.
					PlanTableSource lhs_plan = qtsp.JoinAllPlansWithVariables(lhs_vars);
					PlanTableSource rhs_plan = qtsp.JoinAllPlansWithVariables(rhs_vars);

					// If the lhs and rhs plans are different (there is a joining
					// situation).
					if (lhs_plan != rhs_plan) {

						// If either the LHS or the RHS is a single variable then we can
						// optimize the join.

						if (lhs_v != null || rhs_v != null) {
							// If rhs_v is a single variable and lhs_v is not then we must
							// reverse the expression.
							QueryPlan.JoinNode join_node;
							if (lhs_v == null && rhs_v != null) {
								// Reverse the expressions and the operator
								join_node = new QueryPlan.JoinNode(
											  rhs_plan.Plan, lhs_plan.Plan,
											  rhs_v, op.Reverse(), exps[0]);
								qtsp.MergeTables(rhs_plan, lhs_plan, join_node);
							} else {
								// Otherwise, use it as it is.
								join_node = new QueryPlan.JoinNode(
											  lhs_plan.Plan, rhs_plan.Plan,
											  lhs_v, op, exps[1]);
								qtsp.MergeTables(lhs_plan, rhs_plan, join_node);
							}
							// Return because we are done
							return;
						}

					} // if lhs and rhs plans are different

					// If we get here either both the lhs and rhs are complex expressions
					// or the lhs and rhs of the variable are not different plans, or
					// the operator is not a conditional.  Either way, we must evaluate
					// this via a natural join of the variables involved coupled with an
					// exhaustive select.  These types of queries are poor performing.

					// Get all the variables in the expression
					IList all_vars = expression.AllVariables;
					// Merge it into one plan (possibly performing natural joins).
					PlanTableSource all_plan = qtsp.JoinAllPlansWithVariables(all_vars);
					// And perform the exhaustive select,
					all_plan.UpdatePlan(new QueryPlan.ExhaustiveSelectNode(
													   all_plan.Plan, expression));
				}
			}

			private class SubLogicExpressionPlan : ExpressionPlan {
				private QueryTableSetPlanner qtsp;
				private Expression expression;
				public SubLogicExpressionPlan(QueryTableSetPlanner qtsp, Expression e) {
					this.qtsp = qtsp;
					this.expression = e;
				}
				public override void AddToPlanTree() {
					qtsp.PlanForExpression(expression);
				}
			}


			/// <summary>
			/// Evaluates a list of constant conditional exressions of 
			/// the form <c>3 + 2 = 0</c>, <c>true = true</c>, etc.
			/// </summary>
			/// <param name="constant_vars"></param>
			/// <param name="evaluate_order"></param>
			private void EvaluateConstants(IList constant_vars, IList evaluate_order) {
				// For each constant variable
				for (int i = 0; i < constant_vars.Count; ++i) {
					Expression expr = (Expression)constant_vars[i];
					// Add the exression plan
					ExpressionPlan exp_plan = new ConstantExpressionPlan(this, expr);
					exp_plan.OptimizableValue = 0f;
					evaluate_order.Add(exp_plan);
				}
			}

			/// <summary>
			/// Evaluates a list of single variable conditional expressions 
			/// of the form <c>a = 3</c>, <c>a &gt; 1 + 2</c>, <c>a - 2 = 1</c>, 
			/// <c>3 = a</c>, <c>concat(a, 'a') = '3a'</c>, etc.
			/// </summary>
			/// <param name="single_vars"></param>
			/// <param name="evaluate_order"></param>
			/// <remarks>
			/// The rule is there must be only one variable, a conditional 
			/// operator, and a constant on one side.
			/// <para>
			/// This method takes the list and modifies the plan as 
			/// necessary.
			/// </para>
			/// </remarks>
			private void EvaluateSingles(IList single_vars, IList evaluate_order) {
				// The list of simple expression plans (lhs = single)
				ArrayList simple_plan_list = new ArrayList();
				// The list of complex function expression plans (lhs = expression)
				ArrayList complex_plan_list = new ArrayList();

				// For each single variable expression
				for (int i = 0; i < single_vars.Count; ++i) {
					Expression andexp = (Expression)single_vars[i];
					// The operator
					Operator op = (Operator)andexp.Last;

					// Split the expression
					Expression[] exps = andexp.Split();
					// The single var
					VariableName single_var;

					// If the operator is a sub-command we must be of the form,
					// 'a in ( 1, 2, 3 )'
					if (op.IsSubQuery) {
						single_var = exps[0].VariableName;
						if (single_var != null) {
							ExpressionPlan exp_plan = new SimpleSelectExpressionPlan(this,
																	 single_var, op, exps[1]);
							exp_plan.OptimizableValue = 0.2f;
							evaluate_order.Add(exp_plan);
						} else {
							single_var = (VariableName)exps[0].AllVariables[0];
							ExpressionPlan exp_plan = new ComplexSingleExpressionPlan(this,
																		single_var, andexp);
							exp_plan.OptimizableValue = 0.8f;
							evaluate_order.Add(exp_plan);
						}
					} else {
						// Put the variable on the LHS, constant on the RHS
						IList all_vars = exps[0].AllVariables;
						if (all_vars.Count == 0) {
							// Reverse the expressions and the operator
							Expression temp_exp = exps[0];
							exps[0] = exps[1];
							exps[1] = temp_exp;
							op = op.Reverse();
							single_var = (VariableName)exps[0].AllVariables[0];
						} else {
							single_var = (VariableName)all_vars[0];
						}
						// The table source
						PlanTableSource table_source = FindTableSource(single_var);
						// Simple LHS?
						VariableName v = exps[0].VariableName;
						if (v != null) {
							AddSingleVarPlanTo(simple_plan_list, table_source, v,
											   single_var, exps, op);
						} else {
							// No, complex lhs
							AddSingleVarPlanTo(complex_plan_list, table_source, null,
											   single_var, exps, op);
						}
					}
				}

				// We now have a list of simple and complex plans for each table,
				int sz = simple_plan_list.Count;
				for (int i = 0; i < sz; ++i) {
					SingleVarPlan var_plan = (SingleVarPlan)simple_plan_list[i];
					ExpressionPlan exp_plan = new SimpleSingleExpressionPlan(this,
											   var_plan.single_var, var_plan.expression);
					exp_plan.OptimizableValue = 0.2f;
					evaluate_order.Add(exp_plan);
				}

				sz = complex_plan_list.Count;
				for (int i = 0; i < sz; ++i) {
					SingleVarPlan var_plan = (SingleVarPlan)complex_plan_list[i];
					ExpressionPlan exp_plan = new ComplexSingleExpressionPlan(this,
											   var_plan.single_var, var_plan.expression);
					exp_plan.OptimizableValue = 0.8f;
					evaluate_order.Add(exp_plan);
				}

			}

			/// <summary>
			/// Evaluates a list of expressions that are pattern searches (eg. LIKE, 
			/// NOT LIKE and REGEXP).
			/// </summary>
			/// <param name="pattern_exprs"></param>
			/// <param name="evaluate_order"></param>
			/// <remarks>
			/// The LHS or RHS may be complex expressions with variables, but we are 
			/// guarenteed that there are no sub-expressions in the expression.
			/// </remarks>
			private void EvaluatePatterns(IList pattern_exprs, IList evaluate_order) {

				// Split the patterns into simple and complex plans.  A complex plan
				// may require that a join occurs.

				for (int i = 0; i < pattern_exprs.Count; ++i) {
					Expression expr = (Expression)pattern_exprs[i];

					Expression[] exps = expr.Split();
					// If the LHS is a single variable and the RHS is a constant then
					// the conditions are right for a simple pattern search.
					VariableName lhs_v = exps[0].VariableName;
					if (expr.IsConstant) {
						ExpressionPlan expr_plan = new ConstantExpressionPlan(this, expr);
						expr_plan.OptimizableValue = 0f;
						evaluate_order.Add(expr_plan);
					} else if (lhs_v != null && exps[1].IsConstant) {
						ExpressionPlan expr_plan =
											   new SimplePatternExpressionPlan(this, lhs_v, expr);
						expr_plan.OptimizableValue = 0.25f;
						evaluate_order.Add(expr_plan);
					} else {
						// Otherwise we must assume a complex pattern search which may
						// require a join.  For example, 'a + b LIKE 'a%'' or
						// 'a LIKE b'.  At the very least, this will be an exhaustive
						// search and at the worst it will be a join + exhaustive search.
						// So we should evaluate these at the end of the evaluation order.
						ExpressionPlan expr_plan = new ExhaustiveSelectExpressionPlan(this, expr);
						expr_plan.OptimizableValue = 0.82f;
						evaluate_order.Add(expr_plan);
					}

				}

			}

			/// <summary>
			/// Evaluates a list of expressions containing sub-queries.
			/// </summary>
			/// <param name="expressions"></param>
			/// <param name="evaluate_order"></param>
			/// <remarks>
			/// Non-correlated sub-queries can often be optimized in to fast 
			/// searches.  Correlated queries, or expressions containing multiple 
			/// sub-queries are write through the <see cref="ExhaustiveSelectExpressionPlan"/>.
			/// </remarks>
			private void EvaluateSubQueries(IList expressions, IList evaluate_order) {

				// For each sub-command expression
				for (int i = 0; i < expressions.Count; ++i) {
					Expression andexp = (Expression)expressions[i];

					bool is_exhaustive;
					VariableName left_var = null;
					IQueryPlanNode right_plan = null;

					// Is this an easy sub-command?
					Operator op = (Operator)andexp.Last;
					if (op.IsSubQuery) {
						// Split the expression.
						Expression[] exps = andexp.Split();
						// Check that the left is a simple enough variable reference
						left_var = exps[0].VariableName;
						if (left_var != null) {
							// Check that the right is a sub-command plan.
							right_plan = exps[1].QueryPlanNode;
							if (right_plan != null) {
								// Finally, check if the plan is correlated or not
								ArrayList cv =
									  right_plan.DiscoverCorrelatedVariables(1, new ArrayList());
								//              Console.Out.WriteLine("Right Plan: " + right_plan);
								//              Console.Out.WriteLine("Correlated variables: " + cv);
								if (cv.Count == 0) {
									// No correlated variables so we are a standard, non-correlated
									// command!
									is_exhaustive = false;
								} else {
									is_exhaustive = true;
								}
							} else {
								is_exhaustive = true;
							}
						} else {
							is_exhaustive = true;
						}
					} else {
						// Must be an exhaustive sub-command
						is_exhaustive = true;
					}

					// If this is an exhaustive operation,
					if (is_exhaustive) {
						// This expression could involve multiple variables, so we may need
						// to join.
						IList all_vars = andexp.AllVariables;

						// Also find all correlated variables.
						int level = 0;
						IList all_correlated =
									andexp.DiscoverCorrelatedVariables(ref level, new ArrayList());
						int sz = all_correlated.Count;

						// If there are no variables (and no correlated variables) then this
						// must be a constant select, For example, 3 in ( select ... )
						if (all_vars.Count == 0 && sz == 0) {
							ExpressionPlan expr_plan = new ConstantExpressionPlan(this, andexp);
							expr_plan.OptimizableValue = 0f;
							evaluate_order.Add(expr_plan);
						} else {

							for (int n = 0; n < sz; ++n) {
								CorrelatedVariable cv =
													(CorrelatedVariable)all_correlated[n];
								all_vars.Add(cv.VariableName);
							}

							// An exhaustive expression plan which might require a join or a
							// slow correlated search.  This should be evaluated after the
							// multiple variables are processed.
							ExpressionPlan exp_plan = new ExhaustiveSubQueryExpressionPlan(this,
																		   all_vars, andexp);
							exp_plan.OptimizableValue = 0.85f;
							evaluate_order.Add(exp_plan);
						}

					} else {

						// This is a simple sub-command expression plan with a single LHS
						// variable and a single RHS sub-command.
						ExpressionPlan exp_plan = new SimpleSubQueryExpressionPlan(this, andexp);
						exp_plan.OptimizableValue = 0.3f;
						evaluate_order.Add(exp_plan);

					}

				} // For each 'and' expression

			}

			/// <summary>
			/// Evaluates a list of expressions containing multiple variable 
			/// expression in the form <c>a = b</c>, <c>a &gt; b + c</c>, 
			/// <c> + 5 * b = 2</c>, etc.
			/// </summary>
			/// <param name="multi_vars"></param>
			/// <param name="evaluate_order"></param>
			/// <remarks>
			/// If an expression represents a simple join condition then 
			/// a join plan is made to the command plan tree. If an expression 
			/// represents a more complex joining condition then an exhaustive 
			/// search must be used.
			/// </remarks>
			private void EvaluateMultiples(IList multi_vars, IList evaluate_order) {

				// FUTURE OPTIMIZATION:
				//   This join order planner is a little primitive in design.  It orders
				//   optimizable joins first and least optimizable last, but does not
				//   take into account other factors that we could use to optimize
				//   joins in the future.

				// For each single variable expression
				for (int i = 0; i < multi_vars.Count; ++i) {

					// Get the expression with the multiple variables
					Expression expr = (Expression)multi_vars[i];
					Expression[] exps = expr.Split();

					// Get the list of variables in the left hand and right hand side
					VariableName lhs_v = exps[0].VariableName;
					VariableName rhs_v = exps[1].VariableName;

					// Work out how optimizable the join is.
					// The calculation is as follows;
					// a) If both the lhs and rhs are a single variable then the
					//    optimizable value is set to 0.6f.
					// b) If only one of lhs or rhs is a single variable then the
					//    optimizable value is set to 0.64f.
					// c) Otherwise it is set to 0.68f (exhaustive select guarenteed).

					if (lhs_v == null && rhs_v == null) {
						// Neither lhs or rhs are single vars
						ExpressionPlan exp_plan = new ExhaustiveJoinExpressionPlan(this, expr);
						exp_plan.OptimizableValue = 0.68f;
						evaluate_order.Add(exp_plan);
					} else if (lhs_v != null && rhs_v != null) {
						// Both lhs and rhs are a single var (most optimizable type of
						// join).
						ExpressionPlan exp_plan = new StandardJoinExpressionPlan(this, expr);
						exp_plan.OptimizableValue = 0.60f;
						evaluate_order.Add(exp_plan);
					} else {
						// Either lhs or rhs is a single var
						ExpressionPlan exp_plan = new StandardJoinExpressionPlan(this, expr);
						exp_plan.OptimizableValue = 0.64f;
						evaluate_order.Add(exp_plan);
					}

				} // for each expression we are 'and'ing against

			}

			/// <summary>
			/// Evaluates a list of expressions that are sub-expressions 
			/// themselves.
			/// </summary>
			/// <param name="sublogic_exprs"></param>
			/// <param name="evaluate_order"></param>
			/// <remarks>
			/// This is typically called when we have OR queries in the 
			/// expression.
			/// </remarks>
			private void EvaluateSubLogic(IList sublogic_exprs, IList evaluate_order) {
				//each_logic_expr:
				for (int i = 0; i < sublogic_exprs.Count; ++i) {
					Expression expr = (Expression)sublogic_exprs[i];

					// Break the expression down to a list of OR expressions,
					IList or_exprs = expr.BreakByOperator(new ArrayList(), "or");

					// An optimizations here;

					// If all the expressions we are ORing together are in the same table
					// then we should execute them before the joins, otherwise they
					// should go after the joins.

					// The reason for this is because if we can lesson the amount of work a
					// join has to do then we should.  The actual time it takes to perform
					// an OR search shouldn't change if it is before or after the joins.

					PlanTableSource common = null;

					for (int n = 0; n < or_exprs.Count; ++n) {
						Expression or_expr = (Expression)or_exprs[n];
						IList vars = or_expr.AllVariables;
						// If there are no variables then don't bother with this expression
						if (vars.Count > 0) {
							// Find the common table source (if any)
							PlanTableSource ts = FindCommonTableSource(vars);
							bool or_after_joins = false;
							if (ts == null) {
								// No common table, so OR after the joins
								or_after_joins = true;
							} else if (common == null) {
								common = ts;
							} else if (common != ts) {
								// No common table with the vars in this OR list so do this OR
								// after the joins.
								or_after_joins = true;
							}

							if (or_after_joins) {
								ExpressionPlan exp_plan1 = new SubLogicExpressionPlan(this, expr);
								exp_plan1.OptimizableValue = 0.70f;
								evaluate_order.Add(exp_plan1);
								// Continue to the next logic expression
								//TODO: check this... continue each_logic_expr;
								break;
							}
						}
					}

					// Either we found a common table or there are no variables in the OR.
					// Either way we should evaluate this after the join.
					ExpressionPlan exp_plan = new SubLogicExpressionPlan(this, expr);
					exp_plan.OptimizableValue = 0.58f;
					evaluate_order.Add(exp_plan);
				}
			}


			// -----

			/// <summary>
			/// Generates a plan to evaluate the given list of expressions
			/// (logically separated with AND).
			/// </summary>
			/// <param name="and_list"></param>
			private void PlanForExpressionList(IList and_list) {
				ArrayList sub_logic_expressions = new ArrayList();
				// The list of expressions that have a sub-select in them.
				ArrayList sub_query_expressions = new ArrayList();
				// The list of all constant expressions ( true = true )
				ArrayList constants = new ArrayList();
				// The list of pattern matching expressions (eg. 't LIKE 'a%')
				ArrayList pattern_expressions = new ArrayList();
				// The list of all expressions that are a single variable on one
				// side, a conditional operator, and a constant on the other side.
				ArrayList single_vars = new ArrayList();
				// The list of multi variable expressions (possible joins)
				ArrayList multi_vars = new ArrayList();

				// Separate out each condition type.
				for (int i = 0; i < and_list.Count; ++i) {
					Object el = and_list[i];
					Expression andexp = (Expression)el;
					// If we end with a logical operator then we must recurse them
					// through this method.
					Object lob = andexp.Last;
					Operator op;
					// If the last is not an operator, then we imply
					// '[expression] = true'
					if (!(lob is Operator) ||
						((Operator)lob).IsMathematical) {
						Operator EQUAL_OP = Operator.Get("=");
						andexp.AddElement(TObject.CreateBoolean(true));
						andexp.AddOperator(EQUAL_OP);
						op = EQUAL_OP;
					} else {
						op = (Operator)lob;
					}
					// If the last is logical (eg. AND, OR) then we must process the
					// sub logic expression
					if (op.IsLogical) {
						sub_logic_expressions.Add(andexp);
					}
						// Does the expression have a sub-command?  (eg. Another select
						//   statement somewhere in it)
					else if (andexp.HasSubQuery) {
						sub_query_expressions.Add(andexp);
					} else if (op.IsPattern) {
						pattern_expressions.Add(andexp);
					} else { //if (op.isCondition()) {
						// The list of variables in the expression.
						IList vars = andexp.AllVariables;
						if (vars.Count == 0) {
							// These are ( 54 + 9 = 9 ), ( "z" > "a" ), ( 9.01 - 2 ), etc
							constants.Add(andexp);
						} else if (vars.Count == 1) {
							// These are ( id = 90 ), ( 'a' < number ), etc
							single_vars.Add(andexp);
						} else if (vars.Count > 1) {
							// These are ( id = part_id ),
							// ( cost_of + value_of < sold_at ), ( id = part_id - 10 )
							multi_vars.Add(andexp);
						} else {
							throw new ApplicationException("Hmm, vars list size is negative!");
						}
					}
				}

				// The order in which expression are evaluated,
				// (ExpressionPlan)
				ArrayList evaluate_order = new ArrayList();

				// Evaluate the constants.  These should always be evaluated first
				// because they always evaluate to either true or false or null.
				EvaluateConstants(constants, evaluate_order);

				// Evaluate the singles.  If formed well these can be evaluated
				// using fast indices.  eg. (a > 9 - 3) is more optimal than
				// (a + 3 > 9).
				EvaluateSingles(single_vars, evaluate_order);

				// Evaluate the pattern operators.  Note that some patterns can be
				// optimized better than others, but currently we keep this near the
				// middle of our evaluation sequence.
				EvaluatePatterns(pattern_expressions, evaluate_order);

				// Evaluate the sub-queries.  These are queries of the form,
				// (a IN ( SELECT ... )), (a = ( SELECT ... ) = ( SELECT ... )), etc.
				EvaluateSubQueries(sub_query_expressions, evaluate_order);

				// Evaluate multiple variable expressions.  It's possible these are
				// joins.
				EvaluateMultiples(multi_vars, evaluate_order);

				// Lastly evaluate the sub-logic expressions.  These expressions are
				// OR type expressions.
				EvaluateSubLogic(sub_logic_expressions, evaluate_order);



				// Sort the evaluation list by how optimizable the expressions are,
				evaluate_order.Sort();
				// And add each expression to the plan
				for (int i = 0; i < evaluate_order.Count; ++i) {
					ExpressionPlan plan = (ExpressionPlan)evaluate_order[i];
					plan.AddToPlanTree();
				}

			}

			/// <summary>
			/// Evaluates the search Expression clause and alters the 
			/// banches of the plans in this object as necessary.
			/// </summary>
			/// <param name="exp"></param>
			/// <remarks>
			/// Unlike <see cref="LogicalEvaluate"/>, this does not result 
			/// in a single <see cref="IQueryPlanNode"/>.
			/// It is the responsibility of the callee to join branches 
			/// as required.
			/// </remarks>
			private void PlanForExpression(Expression exp) {
				if (exp == null) {
					return;
				}

				Object ob = exp.Last;
				if (ob is Operator && ((Operator)ob).IsLogical) {
					Operator last_op = (Operator)ob;

					if (last_op.IsEquivalent("or")) {
						// parsing an OR block
						// Split left and right of logical operator.
						Expression[] exps = exp.Split();
						// If we are an 'or' then evaluate left and right and union the
						// result.

						// Before we branch set cache points.
						SetCachePoints();

						// Make copies of the left and right planner
						QueryTableSetPlanner left_planner = Copy();
						QueryTableSetPlanner right_planner = Copy();

						// Plan the left and right side of the OR
						left_planner.PlanForExpression(exps[0]);
						right_planner.PlanForExpression(exps[1]);

						// Fix the left and right planner so that they represent the same
						// 'group'.
						// The current implementation naturally joins all sources if the
						// number of sources is different than the original size.
						int left_sz = left_planner.table_list.Count;
						int right_sz = right_planner.table_list.Count;
						if (left_sz != right_sz ||
							left_planner.HasJoinOccured ||
							right_planner.HasJoinOccured) {
							// Naturally join all in the left and right plan
							left_planner.NaturalJoinAll();
							right_planner.NaturalJoinAll();
						}

						// Union all table sources, but only if they have changed.
						ArrayList left_table_list = left_planner.table_list;
						ArrayList right_table_list = right_planner.table_list;
						int sz = left_table_list.Count;

						// First we must determine the plans that need to be joined in the
						// left and right plan.
						ArrayList left_join_list = new ArrayList();
						ArrayList right_join_list = new ArrayList();
						for (int i = 0; i < sz; ++i) {
							PlanTableSource left_plan =
												  (PlanTableSource)left_table_list[i];
							PlanTableSource right_plan =
												 (PlanTableSource)right_table_list[i];
							if (left_plan.IsUpdated || right_plan.IsUpdated) {
								left_join_list.Add(left_plan);
								right_join_list.Add(right_plan);
							}
						}

						// Make sure the plans are joined in the left and right planners
						left_planner.JoinAllPlansToSingleSource(left_join_list);
						right_planner.JoinAllPlansToSingleSource(right_join_list);

						// Since the planner lists may have changed we update them here.
						left_table_list = left_planner.table_list;
						right_table_list = right_planner.table_list;
						sz = left_table_list.Count;

						ArrayList new_table_list = new ArrayList(sz);

						for (int i = 0; i < sz; ++i) {
							PlanTableSource left_plan =
												  (PlanTableSource)left_table_list[i];
							PlanTableSource right_plan =
												 (PlanTableSource)right_table_list[i];

							PlanTableSource new_plan;

							// If left and right plan updated so we need to union them
							if (left_plan.IsUpdated || right_plan.IsUpdated) {

								// In many causes, the left and right branches will contain
								//   identical branches that would best be optimized out.

								// Take the left plan, add the logical union to it, and make it
								// the plan for this.
								IQueryPlanNode node = new QueryPlan.LogicalUnionNode(
													 left_plan.Plan, right_plan.Plan);

								// Update the plan in this table list
								left_plan.UpdatePlan(node);

								new_plan = left_plan;
							} else {
								// If the left and right plan didn't update, then use the
								// left plan (it doesn't matter if we use left or right because
								// they are the same).
								new_plan = left_plan;
							}

							// Add the left plan to the new table list we are creating
							new_table_list.Add(new_plan);

						}

						// Set the new table list
						table_list = new_table_list;

					} else if (last_op.IsEquivalent("and")) {
						// parsing an AND block
						// The list of AND expressions that are here
						IList and_list = CreateAndList(new ArrayList(), exp);

						PlanForExpressionList(and_list);

					} else {
						throw new Exception("Unknown logical operator: " + ob);
					}

				} else {
					// Not a logical expression so just plan for this single expression.
					ArrayList exp_list = new ArrayList(1);
					exp_list.Add(exp);
					PlanForExpressionList(exp_list);
				}

			}

			/// <summary>
			/// Evaluates a search Expression clause.
			/// </summary>
			/// <param name="exp"></param>
			/// <remarks>
			/// Note that is some cases this will generate a plan tree 
			/// that has many identical branches that can be optimized out.
			/// </remarks>
			/// <returns></returns>
			private IQueryPlanNode LogicalEvaluate(Expression exp) {

				//      Console.Out.WriteLine("Logical Evaluate: " + exp);

				if (exp == null) {
					// Naturally join everything and return the plan.
					NaturalJoinAll();
					// Return the plan
					return SingleTableSource.Plan;
				}

				// Plan the expression
				PlanForExpression(exp);

				// Naturally join any straggling tables
				NaturalJoinAll();

				// Return the plan
				return SingleTableSource.Plan;
			}




			/// <summary>
			/// Given an Expression, this will return a list of expressions 
			/// that can be safely executed as a set of 'and' operations.
			/// </summary>
			/// <param name="list"></param>
			/// <param name="exp"></param>
			/// <remarks>
			/// For example, an expression of <c>a=9 and b=c and d=2</c> 
			/// would return the list: <i>a=9</i>,<i>b=c</i>, <i>d=2</i>.
			/// <para>
			/// If non 'and' operators are found then the reduction stops.
			/// </para>
			/// </remarks>
			/// <returns></returns>
			private static IList CreateAndList(IList list, Expression exp) {
				return exp.BreakByOperator(list, "and");
			}

			/// <summary>
			/// Evalutes the WHERE clause of the table expression.
			/// </summary>
			/// <param name="search_expression"></param>
			/// <returns></returns>
			internal IQueryPlanNode PlanSearchExpression(SearchExpression search_expression) {
				// First perform all outer tables.
				PlanAllOuterJoins();

				IQueryPlanNode node = LogicalEvaluate(search_expression.FromExpression);
				return node;
			}

			/// <summary>
			/// Makes an exact duplicate copy (deep clone) of this planner object.
			/// </summary>
			/// <returns></returns>
			private QueryTableSetPlanner Copy() {
				QueryTableSetPlanner copy = new QueryTableSetPlanner();
				int sz = table_list.Count;
				for (int i = 0; i < sz; ++i) {
					copy.table_list.Add(((PlanTableSource)table_list[i]).Copy());
				}
				// Copy the left and right links in the PlanTableSource
				for (int i = 0; i < sz; ++i) {
					PlanTableSource src = (PlanTableSource)table_list[i];
					PlanTableSource mod = (PlanTableSource)copy.table_list[i];
					// See how the left plan links to which index,
					if (src.left_plan != null) {
						int n = IndexOfPlanTableSource(src.left_plan);
						mod.SetLeftJoinInfo((PlanTableSource)copy.table_list[n],
											src.left_join_type, src.left_on_expr);
					}
					// See how the right plan links to which index,
					if (src.right_plan != null) {
						int n = IndexOfPlanTableSource(src.right_plan);
						mod.SetRightJoinInfo((PlanTableSource)copy.table_list[n],
											 src.right_join_type, src.right_on_expr);
					}
				}

				return copy;
			}

			internal void printDebugInfo() {
				StringBuilder buf = new StringBuilder();
				buf.Append("PLANNER:\n");
				for (int i = 0; i < table_list.Count; ++i) {
					buf.Append("TABLE " + i + "\n");
					((PlanTableSource)table_list[i]).Plan.DebugString(2, buf);
					buf.Append("\n");
				}
				Console.Out.WriteLine(buf.ToString());
			}
		}

		/// <summary>
		/// Represents a single table source being planned.
		/// </summary>
		private sealed class PlanTableSource {
			/// <summary>
			/// The Plan for this table source.
			/// </summary>
			internal IQueryPlanNode plan;

			/// <summary>
			/// The list of fully qualified Variable objects that are accessable 
			/// within this plan.
			/// </summary>
			internal readonly VariableName[] var_list;

			/// <summary>
			/// The list of unique key names of the tables in this plan.
			/// </summary>
			internal readonly String[] unique_names;

			/// <summary>
			/// Set to true when this source has been updated from when it was
			/// constructed or copied.
			/// </summary>
			private bool is_updated;

			// How this plan is naturally joined to other plans in the source.  A
			// plan either has no dependance, a left or a right dependance, or a left
			// and right dependance.
			internal PlanTableSource left_plan;
			internal PlanTableSource right_plan;
			internal JoinType left_join_type;
			internal JoinType right_join_type;
			internal Expression left_on_expr;
			internal Expression right_on_expr;


			public PlanTableSource(IQueryPlanNode plan, VariableName[] var_list,
								   String[] table_unique_names) {
				this.plan = plan;
				this.var_list = var_list;
				this.unique_names = table_unique_names;
				left_join_type = JoinType.None;
				right_join_type = JoinType.None;
				is_updated = false;
			}

			/// <summary>
			/// Sets the left join information for this plan.
			/// </summary>
			/// <param name="left_plan"></param>
			/// <param name="join_type"></param>
			/// <param name="on_expr"></param>
			internal void SetLeftJoinInfo(PlanTableSource left_plan, JoinType join_type, Expression on_expr) {
				this.left_plan = left_plan;
				this.left_join_type = join_type;
				this.left_on_expr = on_expr;
			}

			/// <summary>
			/// Sets the right join information for this plan.
			/// </summary>
			/// <param name="right_plan"></param>
			/// <param name="join_type"></param>
			/// <param name="on_expr"></param>
			internal void SetRightJoinInfo(PlanTableSource right_plan,
								  JoinType join_type, Expression on_expr) {
				this.right_plan = right_plan;
				this.right_join_type = join_type;
				this.right_on_expr = on_expr;
			}

			/// <summary>
			/// This is called when two plans are merged together to set 
			/// up the left and right join information for the new plan.
			/// </summary>
			/// <param name="left"></param>
			/// <param name="right"></param>
			/// <remarks>
			/// This sets the left join info from the left plan and the 
			/// right join info from the right plan.
			/// </remarks>
			internal void SetJoinInfoMergedBetween(
									   PlanTableSource left, PlanTableSource right) {

				if (left.right_plan != right) {
					if (left.right_plan != null) {
						SetRightJoinInfo(left.right_plan,
										 left.right_join_type, left.right_on_expr);
						right_plan.left_plan = this;
					}
					if (right.left_plan != null) {
						SetLeftJoinInfo(right.left_plan,
										right.left_join_type, right.left_on_expr);
						left_plan.right_plan = this;
					}
				}
				if (left.left_plan != right) {
					if (left_plan == null && left.left_plan != null) {
						SetLeftJoinInfo(left.left_plan,
										left.left_join_type, left.left_on_expr);
						left_plan.right_plan = this;
					}
					if (right_plan == null && right.right_plan != null) {
						SetRightJoinInfo(right.right_plan,
										 right.right_join_type, right.right_on_expr);
						right_plan.left_plan = this;
					}
				}

			}

			/// <summary>
			/// Returns true if this table source contains the variable reference.
			/// </summary>
			/// <param name="v"></param>
			/// <returns></returns>
			public bool ContainsVariable(VariableName v) {
				//      Console.Out.WriteLine("Looking for: " + v);
				for (int i = 0; i < var_list.Length; ++i) {
					//        Console.Out.WriteLine(var_list[i]);
					if (var_list[i].Equals(v)) {
						return true;
					}
				}
				return false;
			}

			/// <summary>
			/// Checks if the plan contains the given unique table name.
			/// </summary>
			/// <param name="name"></param>
			/// <returns>
			/// Returns <b>true</b> if this table source contains the 
			/// unique table name reference, otherwise <b>false</b>.
			/// </returns>
			public bool ContainsUniqueKey(String name) {
				for (int i = 0; i < unique_names.Length; ++i) {
					if (unique_names[i].Equals(name)) {
						return true;
					}
				}
				return false;
			}

			/// <summary>
			/// Sets the updated flag.
			/// </summary>
			public void SetUpdated() {
				is_updated = true;
			}

			/// <summary>
			/// Updates the plan.
			/// </summary>
			/// <param name="node"></param>
			public void UpdatePlan(IQueryPlanNode node) {
				plan = node;
				SetUpdated();
			}

			/// <summary>
			/// Returns the plan for this table source.
			/// </summary>
			public IQueryPlanNode Plan {
				get { return plan; }
			}

			/// <summary>
			/// Returns true if the planner was updated.
			/// </summary>
			public bool IsUpdated {
				get { return is_updated; }
			}

			/// <summary>
			/// Makes a copy of this table source.
			/// </summary>
			/// <returns></returns>
			public PlanTableSource Copy() {
				return new PlanTableSource(plan, var_list, unique_names);
			}

		}

		/// <summary>
		/// An abstract class that represents an expression to be added 
		/// into a plan.
		/// </summary>
		/// <remarks>
		/// Many sets of expressions can be added into the plan tree in 
		/// any order, however it is often desirable to add some more 
		/// intensive expressions higher up the branches. This object 
		/// allows us to order expressions by optimization value. More 
		/// optimizable expressions are put near the leafs of the plan 
		/// tree and least optimizable and put near the top.
		/// </remarks>
		abstract class ExpressionPlan : IComparable {
			/// <summary>
			/// How optimizable an expression is.
			/// </summary>
			/// <remarks>
			/// A value of 0 indicates most optimizable and 1 
			/// indicates least optimizable.
			/// </remarks>
			private float optimizable_value;

			/// <summary>
			/// Gets or sets the optimizable value for the plan.
			/// </summary>
			public float OptimizableValue {
				set { optimizable_value = value; }
				get { return optimizable_value; }
			}

			/// <summary>
			/// Adds this expression into the plan tree.
			/// </summary>
			public abstract void AddToPlanTree();

			public int CompareTo(Object ob) {
				ExpressionPlan dest_plan = (ExpressionPlan)ob;
				float dest_val = dest_plan.optimizable_value;
				if (optimizable_value > dest_val) {
					return 1;
				} else if (optimizable_value < dest_val) {
					return -1;
				} else {
					return 0;
				}
			}
		}
	}
}