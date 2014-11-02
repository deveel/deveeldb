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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryPlanner : IQueryPlanner {
		private PreparedQuerySelectColumns PrepareSelectColumns(QueryExpressionFrom fromSet, SqlQueryExpression expression, IQueryContext context) {
			// What we are selecting
			var columnSet = new QuerySelectColumns(fromSet);

			// The list of columns being selected.
			var columns = expression.SelectColumns;

			// For each column being selected
			foreach (SelectColumn column in columns) {
				// Is this a glob?  (eg. Part.* )
				if (column.Expression is SqlReferenceExpression &&
					((SqlReferenceExpression)column.Expression).ReferenceName.IsGlob) {
					var columnName = ((SqlReferenceExpression) column.Expression).ReferenceName;

					// Find the columns globbed and add to the 'selectedColumns' result.
					if (columnName.FullName.Equals("*")) {
						columnSet.SelectAllColumnsFromAllSources();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						var tableName = columnName.Parent;
						columnSet.SelectAllColumnsFromSource(tableName);
					}
				} else {
					// Otherwise must be a standard column reference.
					columnSet.SelectSingleColumn(column);
				}
			}  // for each column selected

			// Prepare the column_set,
			return columnSet.Prepare(context);
		}

		public QueryExpressionFrom GenerateExpressionFrom(IQueryPlanContext context, SqlQueryExpression queryExpression) {
			// Get the 'from_clause' from the table expression
			FromClause fromClause = queryExpression.FromClause;

			// Create a TableExpressionFromSet for this table expression
			var fromSet = new QueryExpressionFrom(context.IgnoreCase);

			// Add all tables from the 'fromClause'
			foreach (FromTable fromTable in fromClause.AllTables) {
				string uniqueKey = fromTable.UniqueKey;
				string alias = fromTable.Alias;

				// If this is a sub-command table,
				if (fromTable.IsSubQuery) {
					// eg. FROM ( SELECT id FROM Part )
					var subQuery = fromTable.SubQuery;
					var subQueryFromSet = GenerateExpressionFrom(context, subQuery);

					// The aliased name of the table
					ObjectName aliasTableName = null;
					if (alias != null)
						aliasTableName = new ObjectName(alias);

					var source = new FromTableSubQuerySource(context.IgnoreCase, uniqueKey, subQuery, subQueryFromSet, aliasTableName);

					// Add to list of subquery tables to add to command,
					fromSet.AddTable(source);
				} else {
					// Else must be a standard command table,
					string name = fromTable.Name;

					// Resolve to full table name
					var tableName = context.ResolveTableName(name);

					if (!context.TableExists(tableName))
						throw new InvalidOperationException(String.Format("Table '{0}' was not found.", tableName));

					ObjectName givenName = null;
					if (alias != null)
						givenName = new ObjectName(alias);

					// Get the ITableQueryInfo object for this table name (aliased).
					ITableQueryInfo tableQueryInfo = context.GetQueryInfo(tableName, givenName);
					var source = new FromTableDirectSource(context.IgnoreCase, tableQueryInfo, uniqueKey, givenName, tableName);

					fromSet.AddTable(source);
				}
			}

			// Set up functions, aliases and exposed variables for this from set,

			// For each column being selected
			foreach (SelectColumn col in queryExpression.SelectColumns) {
				// Is this a glob?  (eg. Part.* )
				if (col.IsGlob) {
					// Find the columns globbed and add to the 'selectedColumns' result.
					if (col.IsAll) {
						fromSet.ExposeAllColumns();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						var tableName = col.ParentName;
						fromSet.ExposeColumns(tableName);
					}
				} else {
					// Otherwise must be a standard column reference.  Note that at this
					// time we aren't sure if a column expression is correlated and is
					// referencing an outer source.  This means we can't verify if the
					// column expression is valid or not at this point.

					// If this column is aliased, add it as a function reference to the set.
					string alias = col.Alias;
					var v = col.ReferenceName;
					bool aliasMatchV = (v != null && alias != null && fromSet.CompareStrings(v.Name, alias));
					if (alias != null && !aliasMatchV) {
						fromSet.AddExpression(new ExpressionReference(col.Expression, alias));
						fromSet.ExposeColumn(new ObjectName(alias));
					} else if (v != null) {
						var resolved = fromSet.ResolveReference(v);
						fromSet.ExposeColumn(resolved ?? v);
					} else {
						string funName = col.Expression.ToString();
						fromSet.AddExpression(new ExpressionReference(col.Expression, funName));
						fromSet.ExposeColumn(new ObjectName(funName));
					}
				}

			}

			return fromSet;
		}

		/// <inheritdoc/>
		public IQueryPlanNode PlanQuery(IQueryPlanContext context, SqlQueryExpression queryExpression, QueryExpressionFrom fromSet) {
			throw new NotImplementedException();
		}
	}
}