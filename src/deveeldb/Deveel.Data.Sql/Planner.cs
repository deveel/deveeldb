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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Various methods for forming command plans on SQL queries.
	/// </summary>
	internal sealed partial class Planner {

		/// <summary>
		/// The name of the GROUP BY function table.
		/// </summary>
		private static readonly TableName GroupByFunctionTable = new TableName("FUNCTIONTABLE");


		/// <summary>
		/// Prepares the given SearchExpression object.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="fromSet"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// This goes through each element of the expression. If the 
		/// element is a variable it is qualified.
		/// If the element is a <see cref="TableSelectExpression"/> it's 
		/// converted to a <see cref="SelectStatement"/> object and prepared.
		/// </remarks>
		private static void PrepareSearchExpression(DatabaseConnection db, TableExpressionFromSet fromSet, SearchExpression expression) {
			// This is used to prepare sub-queries and qualify variables in a
			// search expression such as WHERE or HAVING.

			// Prepare the sub-queries first
			expression.Prepare(new ExpressionPreparerImpl(db, fromSet));

			// Then qualify all the variables.  Note that this will not qualify
			// variables in the sub-queries.
			expression.Prepare(fromSet.ExpressionQualifier);
		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			private readonly TableExpressionFromSet fromSet;
			private readonly DatabaseConnection db;

			public ExpressionPreparerImpl(DatabaseConnection db, TableExpressionFromSet fromSet) {
				this.db = db;
				this.fromSet = fromSet;
			}

			public bool CanPrepare(Object element) {
				return element is TableSelectExpression;
			}

			public Object Prepare(object element) {
				TableSelectExpression sqlExpression = (TableSelectExpression)element;
				TableExpressionFromSet sqlFromSet = GenerateFromSet(sqlExpression, db);
				sqlFromSet.Parent = fromSet;
				IQueryPlanNode sqlPlan = FormQueryPlan(db, sqlExpression, sqlFromSet, null);
				// Form this into a command plan type
				return new TObject(TType.QueryPlanType, new QueryPlan.CachePointNode(sqlPlan));
			}
		}

		/// <summary>
		/// Given a <i>HAVING</i> clause expression, this will generate 
		/// a new <i>HAVING</i> clause expression with all aggregate 
		/// expressions put into the given extra function list.
		/// </summary>
		/// <param name="havingExpression"></param>
		/// <param name="aggregates"></param>
		/// <param name="context"></param>
		/// <returns></returns>
		private static Expression FilterHavingClause(Expression havingExpression, IList<Expression> aggregates, IQueryContext context) {
			if (havingExpression.Count > 1) {
				Operator op = (Operator) havingExpression.Last;
				// If logical, split and filter the left and right expressions
				Expression[] exps = havingExpression.Split();
				Expression newLeft = FilterHavingClause(exps[0], aggregates, context);
				Expression newRight = FilterHavingClause(exps[1], aggregates, context);
				Expression expr = new Expression(newLeft, op, newRight);
				return expr;
			}

			// Not logical so determine if the expression is an aggregate or not
			if (havingExpression.HasAggregateFunction(context)) {
				// Has aggregate functions so we must WriteByte this expression on the
				// aggregate list.
				aggregates.Add(havingExpression);
				// And substitute it with a variable reference.
				VariableName v = VariableName.Resolve("FUNCTIONTABLE.HAVINGAG_" + aggregates.Count);
				return new Expression(v);
			}

			// No aggregate functions so leave it as is.
			return havingExpression;
		}

		/// <summary>
		/// Given a TableExpression, generates a TableExpressionFromSet object.
		/// </summary>
		/// <param name="selectExpression"></param>
		/// <param name="db"></param>
		/// <remarks>
		/// This object is used to help qualify variable references.
		/// </remarks>
		/// <returns></returns>
		public static TableExpressionFromSet GenerateFromSet(TableSelectExpression selectExpression, DatabaseConnection db) {
			// Get the 'from_clause' from the table expression
			FromClause fromClause = selectExpression.From;

			// Prepares the from_clause joining set.
			fromClause.JoinSet.Prepare(db);

			// Create a TableExpressionFromSet for this table expression
			TableExpressionFromSet fromSet = new TableExpressionFromSet(db.IsInCaseInsensitiveMode);

			// Add all tables from the 'fromClause'
			foreach (FromTable fromTable in fromClause.AllTables) {
				string uniqueKey = fromTable.UniqueKey;
				string alias = fromTable.Alias;

				// If this is a sub-command table,
				if (fromTable.IsSubQueryTable) {
					// eg. FROM ( SELECT id FROM Part )
					TableSelectExpression subQuery = fromTable.TableSelectExpression;
					TableExpressionFromSet subQueryFromSet = GenerateFromSet(subQuery, db);
					// The aliased name of the table
					TableName aliasTableName = null;
					if (alias != null)
						aliasTableName = new TableName(alias);

					FromTableSubQuerySource source = new FromTableSubQuerySource(db.IsInCaseInsensitiveMode, uniqueKey, subQuery,
					                                                             subQueryFromSet, aliasTableName);
					// Add to list of subquery tables to add to command,
					fromSet.AddTable(source);
				} else {
					// Else must be a standard command table,
					string name = fromTable.Name;

					// Resolve to full table name
					TableName tableName = db.ResolveTableName(name);

					if (!db.TableExists(tableName))
						throw new StatementException("Table '" + tableName + "' was not found.");

					TableName givenName = null;
					if (alias != null)
						givenName = new TableName(alias);

					// Get the ITableQueryDef object for this table name (aliased).
					ITableQueryDef tableQueryDef = db.GetTableQueryDef(tableName, givenName);
					FromTableDirectSource source = new FromTableDirectSource(db.IsInCaseInsensitiveMode, tableQueryDef, uniqueKey,
					                                                         givenName, tableName);

					fromSet.AddTable(source);
				}
			} 

			// Set up functions, aliases and exposed variables for this from set,

			// The list of columns being selected.
			List<SelectColumn> columns = selectExpression.Columns;

			// For each column being selected
			foreach (SelectColumn col in columns) {
				// Is this a glob?  (eg. Part.* )
				if (col.glob_name != null) {
					// Find the columns globbed and add to the 'selectedColumns' result.
					if (col.glob_name.Equals("*")) {
						fromSet.ExposeAllColumns();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						string tname = col.glob_name.Substring(0, col.glob_name.IndexOf(".*"));
						TableName tn = TableName.Resolve(tname);
						fromSet.ExposeAllColumnsFromSource(tn);
					}
				} else {
					// Otherwise must be a standard column reference.  Note that at this
					// time we aren't sure if a column expression is correlated and is
					// referencing an outer source.  This means we can't verify if the
					// column expression is valid or not at this point.

					// If this column is aliased, add it as a function reference to the
					// TableExpressionFromSet.
					string alias = col.Alias;
					VariableName v = col.Expression.VariableName;
					bool aliasMatchV = (v != null && alias != null && fromSet.StringCompare(v.Name, alias));
					if (alias != null && !aliasMatchV) {
						fromSet.AddFunctionRef(alias, col.Expression);
						fromSet.ExposeVariable(new VariableName(alias));
					} else if (v != null) {
						VariableName resolved = fromSet.ResolveReference(v);
						fromSet.ExposeVariable(resolved ?? v);
					} else {
						string funName = col.Expression.Text.ToString();
						fromSet.AddFunctionRef(funName, col.Expression);
						fromSet.ExposeVariable(new VariableName(funName));
					}
				}

			}  // for each column selected

			return fromSet;
		}

		/// <summary>
		/// Forms a command plan <see cref="IQueryPlanNode"/> from the given 
		/// <see cref="TableSelectExpression"/> and <see cref="TableExpressionFromSet"/>.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="expression">Describes the <i>SELECT</i> command 
		/// (or sub-command).</param>
		/// <param name="fromSet">Used to resolve expression references.</param>
		/// <param name="orderBy">A list of <see cref="ByColumn"/> objects 
		/// that represent an optional <i>ORDER BY</i> clause. If this is null 
		/// or the list is empty, no ordering is done.</param>
		/// <returns></returns>
		public static IQueryPlanNode FormQueryPlan(DatabaseConnection db, TableSelectExpression expression, TableExpressionFromSet fromSet, IList<ByColumn> orderBy) {
			IQueryContext context = new DatabaseQueryContext(db);

			// ----- Resolve the SELECT list

			// What we are selecting
			QuerySelectColumnSet columnSet = new QuerySelectColumnSet(fromSet);

			// The list of columns being selected.
			List<SelectColumn> columns = expression.Columns;

			// If there are 0 columns selected, then we assume the result should
			// show all of the columns in the result.
			bool doSubsetColumn = (columns.Count != 0);

			// For each column being selected
			foreach (SelectColumn col in columns) {
				// Is this a glob?  (eg. Part.* )
				if (col.glob_name != null) {
					// Find the columns globbed and add to the 'selectedColumns' result.
					if (col.glob_name.Equals("*")) {
						columnSet.SelectAllColumnsFromAllSources();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						string tname = col.glob_name.Substring(0, col.glob_name.IndexOf(".*"));
						TableName tn = TableName.Resolve(tname);
						columnSet.SelectAllColumnsFromSource(tn);
					}
				} else {
					// Otherwise must be a standard column reference.
					columnSet.SelectSingleColumn(col);
				}

			}  // for each column selected

			// Prepare the column_set,
			columnSet.Prepare(context);

			// -----

			// Resolve any numerical references in the ORDER BY list (eg.
			// '1' will be a reference to column 1.

			if (orderBy != null) {
				IList<SelectColumn> preparedColSet = columnSet.SelectedColumns;
				foreach (ByColumn col in orderBy) {
					Expression exp = col.Expression;
					if (exp.Count == 1) {
						object lastElem = exp.Last;
						if (lastElem is TObject) {
							BigNumber bnum = ((TObject)lastElem).ToBigNumber();
							if (bnum.Scale == 0) {
								int colRef = bnum.ToInt32() - 1;
								if (colRef >= 0 && colRef < preparedColSet.Count) {
									SelectColumn scol = preparedColSet[colRef];
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

			QueryTableSetPlanner tablePlanner = new QueryTableSetPlanner();

			for (int i = 0; i < fromSet.SetCount; ++i) {
				IFromTableSource table = fromSet.GetTable(i);
				if (table is FromTableSubQuerySource) {
					// This represents a sub-command in the FROM clause

					FromTableSubQuerySource sqlTable = (FromTableSubQuerySource)table;
					TableSelectExpression sqlExpr = sqlTable.TableExpression;
					TableExpressionFromSet sqlFromSet = sqlTable.FromSet;

					// Form a plan for evaluating the sub-command FROM
					IQueryPlanNode sqlPlan = FormQueryPlan(db, sqlExpr, sqlFromSet, null);

					// The top should always be a SubsetNode,
					if (sqlPlan is QueryPlan.SubsetNode) {
						QueryPlan.SubsetNode subsetNode = (QueryPlan.SubsetNode)sqlPlan;
						subsetNode.SetGivenName(sqlTable.AliasedName);
					} else {
						throw new Exception("Top plan is not a SubsetNode!");
					}

					tablePlanner.AddTableSource(sqlPlan, sqlTable);
				} else if (table is FromTableDirectSource) {
					// This represents a direct referencable table in the FROM clause

					FromTableDirectSource dsTable = (FromTableDirectSource)table;
					IQueryPlanNode dsPlan = dsTable.CreateFetchQueryPlanNode();
					tablePlanner.AddTableSource(dsPlan, dsTable);
				} else {
					throw new Exception("Unknown table source instance: " + table.GetType());
				}

			}

			// -----

			// The WHERE and HAVING clauses
			SearchExpression whereClause = expression.Where;
			SearchExpression havingClause = expression.Having;

			// Look at the join set and resolve the ON Expression to this statement
			JoiningSet joinSet = expression.From.JoinSet;

			// Perform a quick scan and see if there are any outer joins in the
			// expression.
			bool allInnerJoins = true;
			for (int i = 0; i < joinSet.TableCount - 1; ++i) {
				JoinType type = joinSet.GetJoinType(i);
				if (type != JoinType.Inner) {
					allInnerJoins = false;
				}
			}

			// Prepare the joins
			for (int i = 0; i < joinSet.TableCount - 1; ++i) {
				JoinType type = joinSet.GetJoinType(i);
				Expression onExpression = joinSet.GetOnExpression(i);

				if (allInnerJoins) {
					// If the whole join set is inner joins then simply move the on
					// expression (if there is one) to the WHERE clause.
					if (onExpression != null)
						whereClause.AppendExpression(onExpression);
				} else {
					// Not all inner joins,
					if (type == JoinType.Inner && onExpression == null) {
						// Regular join with no ON expression, so no preparation necessary
					} else {
						// Either an inner join with an ON expression, or an outer join with
						// ON expression
						if (onExpression == null)
							throw new Exception("No ON expression in join.");

						// Resolve the on_expression
						onExpression.Prepare(fromSet.ExpressionQualifier);
						// And set it in the planner
						tablePlanner.SetJoinInfoBetweenSources(i, type, onExpression);
					}
				}
			}

			// Prepare the WHERE and HAVING clause, qualifies all variables and
			// prepares sub-queries.
			PrepareSearchExpression(db, fromSet, whereClause);
			PrepareSearchExpression(db, fromSet, havingClause);

			// Any extra Aggregate functions that are part of the HAVING clause that
			// we need to add.  This is a list of a name followed by the expression
			// that contains the aggregate function.
			List<Expression> extraAggregateFunctions = new List<Expression>();
			if (havingClause.FromExpression != null) {
				Expression newHavingClause = FilterHavingClause(havingClause.FromExpression, extraAggregateFunctions, context);
				havingClause.SetFromExpression(newHavingClause);
			}

			// Any GROUP BY functions,
			List<Expression> groupByFunctions = new List<Expression>();

			// Resolve the GROUP BY variable list references in this from set
			IList<ByColumn> groupListIn = expression.GroupBy;
			int gsz = groupListIn.Count;
			VariableName[] groupByList = new VariableName[gsz];
			for (int i = 0; i < gsz; ++i) {
				ByColumn byColumn = groupListIn[i];
				Expression exp = byColumn.Expression;
				// Prepare the group by expression
				exp.Prepare(fromSet.ExpressionQualifier);
				// Is the group by variable a complex expression?
				VariableName v = exp.VariableName;

				Expression groupByExpression;
				if (v == null) {
					groupByExpression = exp;
				} else {
					// Can we dereference the variable to an expression in the SELECT?
					groupByExpression = fromSet.DereferenceAssignment(v);
				}

				if (groupByExpression != null) {
					if (groupByExpression.HasAggregateFunction(context))
						throw new StatementException("Aggregate expression '" + groupByExpression.Text + "' is not allowed in GROUP BY clause.");

					// Complex expression so add this to the function list.
					int groupByFunNum = groupByFunctions.Count;
					groupByFunctions.Add(groupByExpression);
					v = new VariableName(GroupByFunctionTable, "#GROUPBY-" + groupByFunNum);
				}
				groupByList[i] = v;
			}

			// Resolve GROUP MAX variable to a reference in this from set
			VariableName groupmaxColumn = expression.GroupMax;
			if (groupmaxColumn != null) {
				VariableName v = fromSet.ResolveReference(groupmaxColumn);
				if (v == null)
					throw new StatementException("Could find GROUP MAX reference '" + groupmaxColumn + "'");

				groupmaxColumn = v;
			}

			// -----

			// Now all the variables should be resolved and correlated variables set
			// up as appropriate.

			// If nothing in the FROM clause then simply evaluate the result of the
			// select
			if (fromSet.SetCount == 0) {
				if (columnSet.aggregate_count > 0)
					throw new StatementException("Invalid use of aggregate function in select with no FROM clause");

				// Make up the lists
				IList<SelectColumn> sColList1 = columnSet.SelectedColumns;
				int sz1 = sColList1.Count;
				string[] colNames = new string[sz1];
				Expression[] expList = new Expression[sz1];
				VariableName[] subsetVars = new VariableName[sz1];
				VariableName[] aliases1 = new VariableName[sz1];
				for (int i = 0; i < sz1; ++i) {
					SelectColumn scol = sColList1[i];
					expList[i] = scol.Expression;
					colNames[i] = scol.internal_name.Name;
					subsetVars[i] = new VariableName(scol.internal_name);
					aliases1[i] = new VariableName(scol.resolved_name);
				}

				return
					new QueryPlan.SubsetNode(
						new QueryPlan.CreateFunctionsNode(new QueryPlan.SingleRowTableNode(), expList, colNames), subsetVars, aliases1);
			}

			// Plan the where clause.  The returned node is the plan to evaluate the
			// WHERE clause.
			IQueryPlanNode node = tablePlanner.PlanSearchExpression(expression.Where);

			// Make up the functions list,
			IList<SelectColumn> functionsList = columnSet.FunctionColumns;
			int fsz = functionsList.Count;
			List<object> completeFunList = new List<object>();
			foreach (SelectColumn scol in functionsList) {
				completeFunList.Add(scol.Expression);
				completeFunList.Add(scol.internal_name.Name);
			}
			for (int i = 0; i < extraAggregateFunctions.Count; ++i) {
				completeFunList.Add(extraAggregateFunctions[i]);
				completeFunList.Add("HAVINGAG_" + (i + 1));
			}

			int fsz2 = completeFunList.Count / 2;
			Expression[] defFunList = new Expression[fsz2];
			string[] defFunNames = new string[fsz2];
			for (int i = 0; i < fsz2; ++i) {
				defFunList[i] = (Expression)completeFunList[i * 2];
				defFunNames[i] = (string)completeFunList[(i * 2) + 1];
			}

			// If there is more than 1 aggregate function or there is a group by
			// clause, then we must add a grouping plan.
			if (columnSet.aggregate_count > 0 || gsz > 0) {
				// If there is no GROUP BY clause then assume the entire result is the
				// group.
				if (gsz == 0) {
					node = new QueryPlan.GroupNode(node, groupmaxColumn, defFunList, defFunNames);
				} else {
					// Do we have any group by functions that need to be planned first?
					int gfsz = groupByFunctions.Count;
					if (gfsz > 0) {
						Expression[] groupFunList = new Expression[gfsz];
						string[] groupFunName = new String[gfsz];
						for (int i = 0; i < gfsz; ++i) {
							groupFunList[i] = groupByFunctions[i];
							groupFunName[i] = "#GROUPBY-" + i;
						}
						node = new QueryPlan.CreateFunctionsNode(node, groupFunList, groupFunName);
					}

					// Otherwise we provide the 'groupByList' argument
					node = new QueryPlan.GroupNode(node, groupByList, groupmaxColumn, defFunList, defFunNames);
				}
			} else {
				// Otherwise no grouping is occuring.  We simply need create a function
				// node with any functions defined in the SELECT.
				// Plan a FunctionsNode with the functions defined in the SELECT.
				if (fsz > 0)
					node = new QueryPlan.CreateFunctionsNode(node, defFunList, defFunNames);
			}

			// The result column list
			IList<SelectColumn> selctedColumns = columnSet.SelectedColumns;
			int sz = selctedColumns.Count;

			// Evaluate the having clause if necessary
			if (expression.Having.FromExpression != null) {
				// Before we evaluate the having expression we must substitute all the
				// aliased variables.
				Expression havingExpr = havingClause.FromExpression;
				SubstituteAliasedVariables(havingExpr, selctedColumns);

				PlanTableSource source = tablePlanner.SingleTableSource;
				source.UpdatePlan(node);
				node = tablePlanner.PlanSearchExpression(havingClause);
			}

			// Do we have a composite select expression to process?
			IQueryPlanNode rightComposite = null;
			if (expression.NextComposite != null) {
				TableSelectExpression compositeExpr = expression.NextComposite;
				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet compositeFromSet = GenerateFromSet(compositeExpr, db);

				// Form the right plan
				rightComposite = FormQueryPlan(db, compositeExpr, compositeFromSet, null);
			}

			// Do we do a final subset column?
			VariableName[] aliases = null;
			if (doSubsetColumn) {
				// Make up the lists
				VariableName[] subsetVars = new VariableName[sz];
				aliases = new VariableName[sz];
				for (int i = 0; i < sz; ++i) {
					SelectColumn scol = selctedColumns[i];
					subsetVars[i] = new VariableName(scol.internal_name);
					aliases[i] = new VariableName(scol.resolved_name);
				}

				// If we are distinct then add the DistinctNode here
				if (expression.Distinct)
					node = new QueryPlan.DistinctNode(node, subsetVars);

				// Process the ORDER BY?
				// Note that the ORDER BY has to occur before the subset call, but
				// after the distinct because distinct can affect the ordering of the
				// result.
				if (rightComposite == null && orderBy != null)
					node = PlanForOrderBy(node, orderBy, fromSet, selctedColumns);

				// Rename the columns as specified in the SELECT
				node = new QueryPlan.SubsetNode(node, subsetVars, aliases);
			} else {
				// Process the ORDER BY?
				if (rightComposite == null && orderBy != null)
					node = PlanForOrderBy(node, orderBy, fromSet, selctedColumns);
			}

			// Do we have a composite to merge in?
			if (rightComposite != null) {
				// For the composite
				node = new QueryPlan.CompositeNode(node, rightComposite, expression.CompositeFunction, expression.IsCompositeAll);
				// Final order by?
				if (orderBy != null) {
					node = PlanForOrderBy(node, orderBy, fromSet, selctedColumns);
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
		/// <param name="orderBy"></param>
		/// <param name="fromSet"></param>
		/// <param name="selctedColumns"></param>
		/// <remarks>
		/// This is given its own function because we may want to plan 
		/// this at the end of a number of composite functions.
		/// </remarks>
		/// <returns></returns>
		public static IQueryPlanNode PlanForOrderBy(IQueryPlanNode plan, IList<ByColumn> orderBy, TableExpressionFromSet fromSet, IList<SelectColumn> selctedColumns) {
			TableName functionTable = new TableName("FUNCTIONTABLE");

			// Sort on the ORDER BY clause
			if (orderBy.Count > 0) {
				int sz = orderBy.Count;
				VariableName[] orderList = new VariableName[sz];
				bool[] ascendingList = new bool[sz];

				List<Expression> functionOrders = new List<Expression>();

				for (int i = 0; i < sz; ++i) {
					ByColumn column = orderBy[i];
					Expression exp = column.Expression;
					ascendingList[i] = column.Ascending;
					VariableName v = exp.VariableName;
					if (v != null) {
						VariableName newV = fromSet.ResolveReference(v);
						if (newV == null)
							throw new StatementException("Can not resolve ORDER BY variable: " + v);

						SubstituteAliasedVariable(newV, selctedColumns);

						orderList[i] = newV;
					} else {
						// Otherwise we must be ordering by an expression such as
						// '0 - a'.

						// Resolve the expression,
						exp.Prepare(fromSet.ExpressionQualifier);

						// Make sure we substitute any aliased columns in the order by
						// columns.
						SubstituteAliasedVariables(exp, selctedColumns);

						// The new ordering functions are called 'FUNCTIONTABLE.#ORDER-n'
						// where n is the number of the ordering expression.
						orderList[i] = new VariableName(functionTable, "#ORDER-" + functionOrders.Count);
						functionOrders.Add(exp);
					}

					//        Console.Out.WriteLine(exp);
				}

				// If there are functional orderings,
				// For this we must define a new FunctionTable with the expressions,
				// then order by those columns, and then use another SubsetNode
				// command node.
				int fsz = functionOrders.Count;
				if (fsz > 0) {
					Expression[] funs = new Expression[fsz];
					String[] fnames = new String[fsz];
					for (int n = 0; n < fsz; ++n) {
						funs[n] = functionOrders[n];
						fnames[n] = "#ORDER-" + n;
					}

					if (plan is QueryPlan.SubsetNode) {
						// If the top plan is a QueryPlan.SubsetNode then we use the
						//   information from it to create a new SubsetNode that
						//   doesn't include the functional orders we have attached here.
						QueryPlan.SubsetNode topSubsetNode = (QueryPlan.SubsetNode)plan;
						VariableName[] mappedNames = topSubsetNode.NewColumnNames;

						// Defines the sort functions
						plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
						// Then plan the sort
						plan = new QueryPlan.SortNode(plan, orderList, ascendingList);
						// Then plan the subset
						plan = new QueryPlan.SubsetNode(plan, mappedNames, mappedNames);
					} else {
						// Defines the sort functions
						plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
						// Plan the sort
						plan = new QueryPlan.SortNode(plan, orderList, ascendingList);
					}

				} else {
					// No functional orders so we only need to sort by the columns
					// defined.
					plan = new QueryPlan.SortNode(plan, orderList, ascendingList);
				}

			}

			return plan;
		}

		/// <summary>
		/// Substitutes any aliased variables in the given expression 
		/// with the function name equivalent.
		/// </summary>
		/// <param name="expression"></param>
		/// <param name="selectColumns"></param>
		/// <remarks>
		/// For example, if we have a <c>SELECT 3 + 4 Bah</c> then resolving 
		/// on variable <i>Bah</i> will be subsituted to the function column
		/// that represents the result of <i>3 + 4</i>.
		/// </remarks>
		private static void SubstituteAliasedVariables(Expression expression, IList<SelectColumn> selectColumns) {
			IList<VariableName> allVars = expression.AllVariables;
			foreach (VariableName v in allVars) {
				SubstituteAliasedVariable(v, selectColumns);
			}
		}

		private static void SubstituteAliasedVariable(VariableName v, IList<SelectColumn> selectColumns) {
			if (selectColumns != null) {
				foreach (SelectColumn scol in selectColumns) {
					if (v.Equals(scol.resolved_name))
						v.Set(scol.internal_name);
				}
			}
		}


		/// <summary>
		/// A container object for the set of SelectColumn objects selected 
		/// in a command.
		/// </summary>
		private sealed class QuerySelectColumnSet {
			/// <summary>
			/// The name of the table where functions are defined.
			/// </summary>
			private static readonly TableName FunctionTableName = new TableName("FUNCTIONTABLE");

			/// <summary>
			/// The tables we are selecting from.
			/// </summary>
			private readonly TableExpressionFromSet fromSet;

			/// <summary>
			/// The list of SelectColumn.
			/// </summary>
			private readonly List<SelectColumn> selectedColumns;

			/// <summary>
			/// The list of functions in this column set.
			/// </summary>
			private readonly List<SelectColumn> functionColumns;

			/// <summary>
			/// The current number of 'FUNCTIONTABLE.' columns in the table.  This is
			/// incremented for each custom column.
			/// </summary>
			private int runningFunNumber = 0;

			// The count of aggregate and constant columns included in the result set.
			// Aggregate columns are, (count(*), avg(cost_of) * 0.75, etc).  Constant
			// columns are, (9 * 4, 2, (9 * 7 / 4) + 4, etc).
			internal int aggregate_count = 0;
			private int constantCount = 0;

			public QuerySelectColumnSet(TableExpressionFromSet fromSet) {
				this.fromSet = fromSet;
				selectedColumns = new List<SelectColumn>();
				functionColumns = new List<SelectColumn>();
			}

			public IList<SelectColumn> SelectedColumns {
				get { return selectedColumns; }
			}

			public IList<SelectColumn> FunctionColumns {
				get { return functionColumns; }
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
			public void SelectSingleColumn(SelectColumn col) {
				selectedColumns.Add(col);
			}

			/// <summary>
			/// Adds all the columns from the given IFromTableSource object.
			/// </summary>
			/// <param name="table"></param>
			private void AddAllFromTable(IFromTableSource table) {
				// Select all the tables
				VariableName[] vars = table.AllColumns;
				foreach (VariableName v in vars) {
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
			public void SelectAllColumnsFromSource(TableName table_name) {
				// Attempt to find the table in the from set.
				IFromTableSource table = fromSet.FindTable(table_name.Schema, table_name.Name);
				if (table == null)
					throw new StatementException(table_name + ".* is not a valid reference.");

				AddAllFromTable(table);
			}

			/// <summary>
			/// Sets up this queriable with all columns from all table 
			/// sources.
			/// </summary>
			public void SelectAllColumnsFromAllSources() {
				for (int p = 0; p < fromSet.SetCount; ++p) {
					IFromTableSource table = fromSet.GetTable(p);
					AddAllFromTable(table);
				}
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
				IList<object> expElements = col.Expression.AllElements;
				foreach (object element in expElements) {
					if (element is TableSelectExpression)
						throw new StatementException("Sub-command not allowed in column list.");
				}

				// First fully qualify the select expression
				col.Expression.Prepare(fromSet.ExpressionQualifier);

				// If the expression isn't a simple variable, then add to
				// function list.
				VariableName v = col.Expression.VariableName;
				if (v == null) {
					// This means we have a complex expression.

					++runningFunNumber;
					string aggStr = runningFunNumber.ToString();

					// If this is an aggregate column then add to aggregate count.
					if (col.Expression.HasAggregateFunction(context)) {
						++aggregate_count;
						// Add '_A' code to end of internal name of column to signify this is
						// an aggregate column
						aggStr += "_A";
					}
						// If this is a constant column then add to constant cound.
					else if (col.Expression.IsConstant) {
						++constantCount;
					} else {
						// Must be an expression with variable's embedded ( eg.
						//   (part_id + 3) * 4, (id * value_of_part), etc )
					}
					functionColumns.Add(col);

					col.internal_name = new VariableName(FunctionTableName, aggStr);
					if (col.Alias == null) {
						col.SetAlias(col.Expression.Text.ToString());
					}
					col.resolved_name = new VariableName(col.Alias);
				} else {
					// Not a complex expression
					col.internal_name = v;
					col.resolved_name = col.Alias == null ? v : new VariableName(col.Alias);
				}
			}


			/// <summary>
			/// Resolves all variable objects in each column.
			/// </summary>
			/// <param name="context"></param>
			public void Prepare(IQueryContext context) {
				// Prepare each of the columns selected.
				// NOTE: A side-effect of this is that it qualifies all the Expressions
				//   that are functions in TableExpressionFromSet.  After this section,
				//   we can dereference variables for their function Expression.
				foreach (SelectColumn column in selectedColumns) {
					PrepareSelectColumn(column, context);
				}
			}
		}
	}
}