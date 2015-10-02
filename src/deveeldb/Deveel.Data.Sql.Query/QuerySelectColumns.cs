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
using System.Diagnostics;
using System.Globalization;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	sealed class QuerySelectColumns {
		/// <summary>
		/// The name of the table where functions are defined.
		/// </summary>
		private static readonly ObjectName FunctionTableName = new ObjectName("FUNCTIONTABLE");

		/// <summary>
		/// The tables we are selecting from.
		/// </summary>
		private readonly QueryExpressionFrom fromSet;

		private readonly List<SelectColumn> selectedColumns;

		// The count of aggregate and constant columns included in the result set.
		// Aggregate columns are, (count(*), avg(cost_of) * 0.75, etc).  Constant
		// columns are, (9 * 4, 2, (9 * 7 / 4) + 4, etc).

		public QuerySelectColumns(QueryExpressionFrom fromSet) {
			this.fromSet = fromSet;
			selectedColumns = new List<SelectColumn>();
		}

		public void SelectSingleColumn(SelectColumn col) {
			selectedColumns.Add(col);
		}

		private void AddAllFromTable(IFromTableSource table) {
			// Select all the tables
			var columns = table.ColumnNames;
			foreach (ObjectName name in columns) {
				// Make up the SelectColumn
				SqlExpression e = SqlExpression.Reference(name);
				var column = new SelectColumn(e) {
					ResolvedName = name, 
					InternalName = name
				};

				// Add to the list of columns selected
				SelectSingleColumn(column);
			}
		}

		public void SelectAllColumnsFromSource(ObjectName tableName) {
			// Attempt to find the table in the from set.
			string schema = null;
			if (tableName.Parent != null)
				schema = tableName.Parent.Name;

			IFromTableSource table = fromSet.FindTable(schema, tableName.Name);
			if (table == null)
				throw new InvalidOperationException(tableName + ".* is not a valid reference.");

			AddAllFromTable(table);
		}

		public void SelectAllColumnsFromAllSources() {
			for (int p = 0; p < fromSet.SourceCount; ++p) {
				IFromTableSource table = fromSet.GetTableSource(p);
				AddAllFromTable(table);
			}
		}

		private SelectColumn PrepareColumn(SelectColumn column, IQueryContext context, IList<SelectColumn> functionColumns,
			ref int aggregateCount) {
			if (column.Expression is SqlQueryExpression)
				throw new InvalidOperationException("Sub-query expressions are invalid in select columns.");

			SelectColumn newColumn;

			var exp = column.Expression;
			if (exp != null)
				exp = exp.Prepare(fromSet.ExpressionPreparer);

			if (exp is SqlReferenceExpression) {
				var sqlRef = (SqlReferenceExpression) exp;
				var colName = sqlRef.ReferenceName;
				ObjectName resolvedName = null;

				var alias = column.Alias;
				if (String.IsNullOrEmpty(alias)) {
					resolvedName = colName;
				} else {
					resolvedName = new ObjectName(alias);
				}

				newColumn = new SelectColumn(exp, alias) {
					InternalName = colName,
					ResolvedName = resolvedName
				};
			} else {
				var funcAlias = functionColumns.Count.ToString(CultureInfo.InvariantCulture);
				if (column.Expression.HasAggregate(context)) {
					aggregateCount++;
					funcAlias += "_A";
				}

				var alias = column.Alias;
				if (string.IsNullOrEmpty(alias))
					alias = exp.ToString();

				newColumn = new SelectColumn(exp, alias) {
					InternalName = new ObjectName(FunctionTableName, funcAlias),
					ResolvedName = new ObjectName(alias)
				};

				functionColumns.Add(newColumn);
			}

			return newColumn;
		}

		public PreparedQuerySelectColumns Prepare(IQueryContext context) {
			int aggregateCount = 0;
			var functionColumns = new List<SelectColumn>();
			var preparedColumns = new List<SelectColumn>();
			foreach (var column in selectedColumns) {
				var prepared = PrepareColumn(column, context, functionColumns, ref aggregateCount);
				preparedColumns.Add(prepared);
			}

			return new PreparedQuerySelectColumns(preparedColumns, functionColumns, aggregateCount);
		}
	}
}