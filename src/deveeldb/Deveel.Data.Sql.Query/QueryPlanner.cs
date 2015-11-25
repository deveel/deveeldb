// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryPlanner : IQueryPlanner {
		private static readonly ObjectName FunctionTableName = new ObjectName("FUNCTIONTABLE");

		private SqlExpression PrepareSearchExpression(IRequest context, QueryExpressionFrom queryFrom, SqlExpression expression) {
			// first check the expression is not null
			if (expression == null)
				return null;

			// This is used to prepare sub-queries and qualify variables in a
			// search expression such as WHERE or HAVING.

			// Prepare the sub-queries first
			expression = expression.Prepare(new QueryExpressionPreparer(this, queryFrom, context));

			// Then qualify all the variables.  Note that this will not qualify
			// variables in the sub-queries.
			expression = expression.Prepare(queryFrom.ExpressionPreparer);

			return expression;
		}

		private QuerySelectColumns BuildSelectColumns(SqlQueryExpression expression, QueryExpressionFrom queryFrom) {
			var selectColumns = new QuerySelectColumns(queryFrom);

			foreach (var column in expression.SelectColumns) {
				// Is this a glob?  (eg. Part.* )
				if (column.IsGlob) {
					// Find the columns globbed and add to the 'selectedColumns' result.
					if (column.IsAll) {
						selectColumns.SelectAllColumnsFromAllSources();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						selectColumns.SelectAllColumnsFromSource(column.TableName);
					}
				} else {
					// Otherwise must be a standard column reference.
					selectColumns.SelectSingleColumn(column);
				}
			}

			return selectColumns;
		}


		private static int MakeupFunctions(PreparedQuerySelectColumns columnSet, IList<SqlExpression> aggregateFunctions, out SqlExpression[] defFunList, out string[] defFunNames) {
			// Make up the functions list,
			var functionsList = columnSet.FunctionColumns.ToList();
			int fsz = functionsList.Count;
			var completeFunList = new List<object>();
			for (int i = 0; i < fsz; ++i) {
				var scol = functionsList[i];
				completeFunList.Add(scol.Expression);
				completeFunList.Add(scol.InternalName.Name);
			}

			for (int i = 0; i < aggregateFunctions.Count; ++i) {
				completeFunList.Add(aggregateFunctions[i]);
				completeFunList.Add("HAVINGAGG_" + (i + 1));
			}

			int fsz2 = completeFunList.Count / 2;
			defFunList = new SqlExpression[fsz2];
			defFunNames = new string[fsz2];
			for (int i = 0; i < fsz2; ++i) {
				defFunList[i] = (SqlExpression)completeFunList[i * 2];
				defFunNames[i] = (string)completeFunList[(i * 2) + 1];
			}

			return fsz;
		}

		private IQueryPlanNode PlanGroup(IQueryPlanNode node, GroupInfo groupInfo) {
			// If there is more than 1 aggregate function or there is a group by
			// clause, then we must add a grouping plan.
			if (groupInfo.Columns.AggregateCount > 0 || 
				groupInfo.GroupByCount > 0) {
				// If there is no GROUP BY clause then assume the entire result is the
				// group.
				if (groupInfo.GroupByCount == 0) {
					node = new GroupNode(node, groupInfo.GroupMax, groupInfo.FunctionExpressions, groupInfo.FunctionNames);
				} else {
					// Do we have any group by functions that need to be planned first?
					int gfsz = groupInfo.GroupByExpressions.Length;
					if (gfsz > 0) {
						var groupFunList = new SqlExpression[gfsz];
						var groupFunName = new string[gfsz];
						for (int i = 0; i < gfsz; ++i) {
							groupFunList[i] = groupInfo.GroupByExpressions[i];
							groupFunName[i] = "#GROUPBY-" + i;
						}

						node = new CreateFunctionsNode(node, groupFunList, groupFunName);
					}

					// Otherwise we provide the 'group_by_list' argument
					node = new GroupNode(node, groupInfo.GroupByNames, groupInfo.GroupMax, groupInfo.FunctionExpressions, groupInfo.FunctionNames);
				}
			} else {
				// Otherwise no grouping is occurring.  We simply need create a function
				// node with any functions defined in the SELECT.
				// Plan a FunctionsNode with the functions defined in the SELECT.
				if (groupInfo.FunctionCount > 0)
					node = new CreateFunctionsNode(node, groupInfo.FunctionExpressions, groupInfo.FunctionNames);
			}

			return node;
		}

		#region GroupInfo

		class GroupInfo {
			public PreparedQuerySelectColumns Columns { get; set; }

			public ObjectName GroupMax { get; set; }

			public int GroupByCount { get; set; }

			public ObjectName[] GroupByNames { get; set; }

			public SqlExpression[] GroupByExpressions { get; set; }

			public int FunctionCount { get; set; }

			public string[] FunctionNames { get; set; }

			public SqlExpression[] FunctionExpressions { get; set; }
		}

		#endregion

		private QueryTablePlanner CreateTablePlanner(IRequest context, QueryExpressionFrom queryFrom) {
			// Set up plans for each table in the from clause of the command.  For
			// sub-queries, we recurse.

			var tablePlanner = new QueryTablePlanner();

			for (int i = 0; i < queryFrom.SourceCount; i++) {
				var tableSource = queryFrom.GetTableSource(i);
				IQueryPlanNode plan;

				if (tableSource is FromTableSubQuerySource) {
					var subQuerySource = (FromTableSubQuerySource) tableSource;

					var subQueryExpr = subQuerySource.QueryExpression;
					var subQueryFrom = subQuerySource.QueryFrom;

					plan = PlanQuery(context, subQueryExpr, subQueryFrom, null, null);

					if (!(plan is SubsetNode))
						throw new InvalidOperationException("The root node of a sub-query plan must be a subset.");

					var subsetNode = (SubsetNode) plan;
					subsetNode.SetAliasParentName(subQuerySource.AliasName);
				} else if (tableSource is FromTableDirectSource) {
					var directSource = (FromTableDirectSource) tableSource;
					plan = directSource.QueryPlan;
				} else {
					throw new InvalidOperationException(String.Format("The type of FROM source '{0}' is not supported.", tableSource.GetType()));
				}

				tablePlanner.AddPlan(plan, tableSource);
			}

			return tablePlanner;
		}

		private void PrepareJoins(QueryTablePlanner tablePlanner, SqlQueryExpression queryExpression, QueryExpressionFrom queryFrom, ref SqlExpression searchExpression) {
			var fromClause = queryExpression.FromClause;

			bool allInner = true;
			for (int i = 0; i < fromClause.JoinPartCount; i++) {
				var joinPart = fromClause.GetJoinPart(i);
				if (joinPart.JoinType != JoinType.Inner)
					allInner = false;
			}

			for (int i = 0; i < fromClause.JoinPartCount; i++) {
				var joinPart = fromClause.GetJoinPart(i);

				var joinType = joinPart.JoinType;
				var onExpression = joinPart.OnExpression;

				if (allInner) {
					// If the whole join set is inner joins then simply move the on
					// expression (if there is one) to the WHERE clause.
					if (searchExpression != null && onExpression != null)
						searchExpression = SqlExpression.And(searchExpression, onExpression);
				} else {
					// Not all inner joins,
					if (joinType == JoinType.Inner && onExpression == null) {
						// Regular join with no ON expression, so no preparation necessary
					} else {
						// Either an inner join with an ON expression, or an outer join with
						// ON expression
						if (onExpression == null)
							throw new InvalidOperationException(String.Format("Join of type {0} requires ON expression.", joinType));

						// Resolve the on_expression
						onExpression = onExpression.Prepare(queryFrom.ExpressionPreparer);
						// And set it in the planner
						tablePlanner.JoinAt(i, joinType, onExpression);
					}
				}
			}
		}

		private SqlExpression FilterHaving(SqlExpression havingExpression, IList<SqlExpression> aggregates, IRequest context) {
			if (havingExpression is SqlBinaryExpression) {
				var binary = (SqlBinaryExpression) havingExpression;
				var expType = binary.ExpressionType;
				var newLeft = FilterHaving(binary.Left, aggregates, context);
				var newRight = FilterHaving(binary.Right, aggregates, context);
				return SqlExpression.Binary(newLeft, expType, newRight);
			}

			// Not logical so determine if the expression is an aggregate or not
			if (havingExpression.HasAggregate(context)) {
				// Has aggregate functions so we must WriteByte this expression on the
				// aggregate list.

				aggregates.Add(havingExpression);

				var name = new ObjectName(FunctionTableName, String.Format("HAVINGAGG_{0}", aggregates.Count));
				return SqlExpression.Reference(name);
			}

			return havingExpression;
		}

		private int ResolveGroupBy(SqlQueryExpression queryExpression, QueryExpressionFrom queryFrom, IRequest context, out ObjectName[] columnNames, out IList<SqlExpression> expressions) {
			var groupBy = queryExpression.GroupBy == null
				? new List<SqlExpression>(0)
				: queryExpression.GroupBy.ToList();
			var groupBySize = groupBy.Count;

			expressions = new List<SqlExpression>();
			columnNames = new ObjectName[groupBySize];

			for (int i = 0; i < groupBySize; i++) {
				var expression = groupBy[i];

				// Prepare the group by expression
				expression = expression.Prepare(queryFrom.ExpressionPreparer);

				var columnName = expression.AsReferenceName();
				if (columnName != null)
					expression = queryFrom.FindExpression(columnName);

				if (expression != null) {
					if (expression.HasAggregate(context))
						throw new InvalidOperationException(String.Format("Aggregate expression '{0}' is not allowed in a GROUP BY clause", expression));

					expressions.Add(expression);
					columnName = new ObjectName(FunctionTableName, String.Format("#GROUPBY-{0}", expressions.Count -1));
				}

				columnNames[i] = columnName;
			}

			return groupBySize;
		}

		private ObjectName ResolveGroupMax(SqlQueryExpression queryExpression, QueryExpressionFrom queryFrom) {
			var groupMax = queryExpression.GroupMax;
			if (groupMax != null) {
				var variable = queryFrom.ResolveReference(groupMax);
				if (variable == null)
					throw new InvalidOperationException(String.Format("The GROUP MAX column '{0}' was not found.", groupMax));

				groupMax = variable;
			}

			return groupMax;
		}

		private IQueryPlanNode EvaluateToSingle(PreparedQuerySelectColumns columns) {
			if (columns.AggregateCount > 0)
				throw new InvalidOperationException("Invalid use of aggregate function in select with no FROM clause");

			// Make up the lists
			var selectedColumns = columns.SelectedColumns.ToList();
			int colCount = selectedColumns.Count;
			var colNames = new string[colCount];
			var expList = new SqlExpression[colCount];
			var subsetVars = new ObjectName[colCount];
			var aliases1 = new ObjectName[colCount];
			for (int i = 0; i < colCount; ++i) {
				SelectColumn scol = selectedColumns[i];
				expList[i] = scol.Expression;
				colNames[i] = scol.InternalName.Name;
				subsetVars[i] = scol.InternalName;
				aliases1[i] = scol.ResolvedName;
			}

			return new SubsetNode(new CreateFunctionsNode(new SingleRowTableNode(), expList, colNames), subsetVars, aliases1);
		}

		private static IList<SortColumn> ResolveOrderByRefs(PreparedQuerySelectColumns columnSet,
			IEnumerable<SortColumn> orderBy) {
			// Resolve any numerical references in the ORDER BY list (eg.
			// '1' will be a reference to column 1.
			if (orderBy == null)
				return null;

			var columnCount = columnSet.SelectedColumns.Count();

			var resolvedColumns = new List<SortColumn>();
			foreach (var column in orderBy) {
				var resolved = column;

				var expression = column.Expression;
				if (expression.ExpressionType == SqlExpressionType.Constant) {
					var value = ((SqlConstantExpression) expression).Value;
					if (value.Type is NumericType &&
					    !value.IsNull) {
						var colRef = ((SqlNumber) value.Value).ToInt32() - 1;
						if (colRef >= 0 && colRef < columnCount) {
							var funArray = columnSet.FunctionColumns.ToArray();
							var refExp = funArray[colRef];

							resolved = new SortColumn(refExp.Expression, column.Ascending);
						}
					}
				}

				resolvedColumns.Add(resolved);
			}

			return resolvedColumns.ToArray();
		}

		public IQueryPlanNode PlanQuery(IRequest context, SqlQueryExpression queryExpression, IEnumerable<SortColumn> sortColumns, QueryLimit limit) {
			var queryFrom = QueryExpressionFrom.Create(context, queryExpression);
			var orderBy = new List<SortColumn>();
			if (sortColumns != null)
				orderBy.AddRange(sortColumns);

			return PlanQuery(context, queryExpression, queryFrom, orderBy, limit);
		}

		private IQueryPlanNode PlanQuery(IRequest context, SqlQueryExpression queryExpression,
			QueryExpressionFrom queryFrom, IList<SortColumn> sortColumns, QueryLimit limit) {

			// ----- Resolve the SELECT list
			// If there are 0 columns selected, then we assume the result should
			// show all of the columns in the result.
			bool doSubsetColumn = (queryExpression.SelectColumns.Any());

			// What we are selecting
			var columns = BuildSelectColumns(queryExpression, queryFrom);

			// Prepare the column_set,
			var preparedColumns = columns.Prepare(context);

			sortColumns = ResolveOrderByRefs(preparedColumns, sortColumns);

			// -----

			// Set up plans for each table in the from clause of the command.  For
			// sub-queries, we recurse.

			var tablePlanner = CreateTablePlanner(context, queryFrom);

			// -----

			// The WHERE and HAVING clauses
			var whereClause = queryExpression.WhereExpression;
			var havingClause = queryExpression.HavingExpression;

			PrepareJoins(tablePlanner, queryExpression, queryFrom, ref whereClause);

			// Prepare the WHERE and HAVING clause, qualifies all variables and
			// prepares sub-queries.
			whereClause = PrepareSearchExpression(context, queryFrom, whereClause);
			havingClause = PrepareSearchExpression(context, queryFrom, havingClause);

			// Any extra Aggregate functions that are part of the HAVING clause that
			// we need to add.  This is a list of a name followed by the expression
			// that contains the aggregate function.
			var extraAggregateFunctions = new List<SqlExpression>();
			if (havingClause != null)
				havingClause = FilterHaving(havingClause, extraAggregateFunctions, context);

			// Any GROUP BY functions,
			ObjectName[] groupByList;
			IList<SqlExpression> groupByFunctions;
			var gsz = ResolveGroupBy(queryExpression, queryFrom, context, out groupByList, out groupByFunctions);

			// Resolve GROUP MAX variable to a reference in this from set
			var groupmaxColumn = ResolveGroupMax(queryExpression, queryFrom);

			// -----

			// Now all the variables should be resolved and correlated variables set
			// up as appropriate.

			// If nothing in the FROM clause then simply evaluate the result of the
			// select
			if (queryFrom.SourceCount == 0)
				return EvaluateToSingle(preparedColumns);

			// Plan the where clause.  The returned node is the plan to evaluate the
			// WHERE clause.
			var node = tablePlanner.PlanSearchExpression(whereClause);

			SqlExpression[] defFunList;
			string[] defFunNames;
			var fsz = MakeupFunctions(preparedColumns, extraAggregateFunctions, out defFunList, out defFunNames);

			var groupInfo = new GroupInfo {
				Columns = preparedColumns,
				FunctionCount = fsz,
				FunctionNames = defFunNames,
				FunctionExpressions = defFunList,
				GroupByCount = gsz,
				GroupByNames = groupByList,
				GroupByExpressions = groupByFunctions.ToArray(),
				GroupMax = groupmaxColumn
			};

			node = PlanGroup(node, groupInfo);

			// The result column list
			var selectColumns = preparedColumns.SelectedColumns.ToList();
			int sz = selectColumns.Count;

			// Evaluate the having clause if necessary
			if (havingClause != null) {
				// Before we evaluate the having expression we must substitute all the
				// aliased variables.
				var havingExpr = havingClause;

				// TODO: this requires a visitor to modify the having expression
				havingExpr = ReplaceAliasedVariables(havingExpr, selectColumns);

				var source = tablePlanner.SinglePlan;
				source.UpdatePlan(node);
				node = tablePlanner.PlanSearchExpression(havingExpr);
			}

			// Do we have a composite select expression to process?
			IQueryPlanNode rightComposite = null;
			if (queryExpression.NextComposite != null) {
				var compositeExpr = queryExpression.NextComposite;
				var compositeFrom = QueryExpressionFrom.Create(context, compositeExpr);

				// Form the right plan
				rightComposite = PlanQuery(context, compositeExpr, compositeFrom, null, null);
			}

			// Do we do a final subset column?
			ObjectName[] aliases = null;
			if (doSubsetColumn) {
				// Make up the lists
				var subsetVars = new ObjectName[sz];
				aliases = new ObjectName[sz];
				for (int i = 0; i < sz; ++i) {
					SelectColumn scol = selectColumns[i];
					subsetVars[i] = scol.InternalName;
					aliases[i] = scol.ResolvedName;
				}

				// If we are distinct then add the DistinctNode here
				if (queryExpression.Distinct)
					node = new DistinctNode(node, subsetVars);

				// Process the ORDER BY?
				// Note that the ORDER BY has to occur before the subset call, but
				// after the distinct because distinct can affect the ordering of the
				// result.
				if (rightComposite == null && sortColumns != null)
					node = PlanForOrderBy(node, sortColumns, queryFrom, selectColumns);

				// Rename the columns as specified in the SELECT
				node = new SubsetNode(node, subsetVars, aliases);
			} else {
				// Process the ORDER BY?
				if (rightComposite == null && sortColumns != null)
					node = PlanForOrderBy(node, sortColumns, queryFrom, selectColumns);
			}

			// Do we have a composite to merge in?
			if (rightComposite != null) {
				// For the composite
				node = new CompositeNode(node, rightComposite, queryExpression.CompositeFunction, queryExpression.IsCompositeAll);

				// Final order by?
				if (sortColumns != null)
					node = PlanForOrderBy(node, sortColumns, queryFrom, selectColumns);

				// Ensure a final subset node
				if (!(node is SubsetNode) && aliases != null) {
					node = new SubsetNode(node, aliases, aliases);
				}
			}

			if (limit != null)
				node = new LimitNode(node, limit.Offset, limit.Count);

			return node;
		}

		private static IQueryPlanNode PlanForOrderBy(IQueryPlanNode plan, IList<SortColumn> orderBy, QueryExpressionFrom queryFrom, IList<SelectColumn> selectedColumns) {
			// Sort on the ORDER BY clause
			if (orderBy.Count > 0) {
				int sz = orderBy.Count;
				var orderList = new ObjectName[sz];
				var ascendingList = new bool[sz];

				var functionOrders = new List<SqlExpression>();

				for (int i = 0; i < sz; ++i) {
					var column = orderBy[i];
					SqlExpression exp = column.Expression;
					ascendingList[i] = column.Ascending;
					var v = exp.AsReferenceName();

					if (v != null) {
						var newV = queryFrom.ResolveReference(v);
						if (newV == null)
							throw new InvalidOperationException(String.Format("Could not resolve ORDER BY column '{0}' in expression", v));

						newV = ReplaceAliasedVariable(newV, selectedColumns);
						orderList[i] = newV;
					} else {
						// Otherwise we must be ordering by an expression such as
						// '0 - a'.

						// Resolve the expression,
						exp = exp.Prepare(queryFrom.ExpressionPreparer);

						// Make sure we substitute any aliased columns in the order by
						// columns.
						exp = ReplaceAliasedVariables(exp, selectedColumns);

						// The new ordering functions are called 'FUNCTIONTABLE.#ORDER-n'
						// where n is the number of the ordering expression.
						orderList[i] = new ObjectName(FunctionTableName, "#ORDER-" + functionOrders.Count);
						functionOrders.Add(exp);
					}
				}

				// If there are functional orderings,
				// For this we must define a new FunctionTable with the expressions,
				// then order by those columns, and then use another SubsetNode
				// command node.
				int fsz = functionOrders.Count;
				if (fsz > 0) {
					var funs = new SqlExpression[fsz];
					var fnames = new String[fsz];
					for (int n = 0; n < fsz; ++n) {
						funs[n] = functionOrders[n];
						fnames[n] = "#ORDER-" + n;
					}

					if (plan is SubsetNode) {
						// If the top plan is a SubsetNode then we use the
						//   information from it to create a new SubsetNode that
						//   doesn't include the functional orders we have attached here.
						var topSubsetNode = (SubsetNode)plan;
						var mappedNames = topSubsetNode.AliasColumnNames;

						// Defines the sort functions
						plan = new CreateFunctionsNode(plan, funs, fnames);
						// Then plan the sort
						plan = new SortNode(plan, orderList, ascendingList);
						// Then plan the subset
						plan = new SubsetNode(plan, mappedNames, mappedNames);
					} else {
						// Defines the sort functions
						plan = new CreateFunctionsNode(plan, funs, fnames);
						// Plan the sort
						plan = new SortNode(plan, orderList, ascendingList);
					}

				} else {
					// No functional orders so we only need to sort by the columns
					// defined.
					plan = new SortNode(plan, orderList, ascendingList);
				}
			}

			return plan;
		}

		private static SqlExpression ReplaceAliasedVariables(SqlExpression expression, IList<SelectColumn> selectedColumns) {
			var replacer = new VariableReplacer(selectedColumns);
			return replacer.Visit(expression);
		}

		private static ObjectName ReplaceAliasedVariable(ObjectName variableName, IEnumerable<SelectColumn> selectColumns) {
			foreach (var column in selectColumns) {
				if (column.ResolvedName.Equals(variableName))
					return column.InternalName;
			}

			return variableName;
		}

		#region QueryExpressionPreparer

		class QueryExpressionPreparer : IExpressionPreparer {
			private readonly QueryPlanner planner;
			private readonly QueryExpressionFrom parent;
			private readonly IRequest context;

			public QueryExpressionPreparer(QueryPlanner planner, QueryExpressionFrom parent, IRequest context) {
				this.planner = planner;
				this.parent = parent;
				this.context = context;
			}

			public bool CanPrepare(SqlExpression expression) {
				return expression is SqlQueryExpression;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				var queryExpression = (SqlQueryExpression) expression;
				var queryFrom = QueryExpressionFrom.Create(context, queryExpression);
				queryFrom.Parent = parent;
				var plan = planner.PlanQuery(context, queryExpression, queryFrom, null, null);
				return SqlExpression.Constant(new DataObject(new QueryType(), new SqlQueryObject(new CachePointNode(plan))));
			}
		}

		#endregion

		#region VariableReplacer

		class VariableReplacer : SqlExpressionVisitor {
			private readonly IEnumerable<SelectColumn> selectColumns;

			public VariableReplacer(IEnumerable<SelectColumn> selectColumns) {
				this.selectColumns = selectColumns;
			}

			public override SqlExpression VisitVariableReference(SqlVariableReferenceExpression reference) {
				// TODO: should we also resolve variables?
				return base.VisitVariableReference(reference);
			}

			public override SqlExpression VisitReference(SqlReferenceExpression reference) {
				var refName = reference.ReferenceName;
				foreach (var column in selectColumns) {
					if (refName.Equals(column.ResolvedName))
						return SqlExpression.Reference(column.InternalName);
				}

				return base.VisitReference(reference);
			}
		}

		#endregion
	}
}
