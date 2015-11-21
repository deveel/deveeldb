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
using System.Reflection;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryExpressionFrom {
		private readonly List<IFromTableSource> tableSources;
		private readonly List<ExpressionReference> expressionReferences;
		private readonly List<ObjectName> exposedColumns;

		public QueryExpressionFrom() 
			: this(true) {
		}

		public QueryExpressionFrom(bool ignoreCase) {
			IgnoreCase = ignoreCase;

			tableSources = new List<IFromTableSource>();
			expressionReferences = new List<ExpressionReference>();
			exposedColumns = new List<ObjectName>();

			ExpressionPreparer = new FromExpressionPreparer(this);
		}

		public bool IgnoreCase { get; private set; }

		public QueryExpressionFrom Parent { get; set; }

		public IExpressionPreparer ExpressionPreparer { get; private set; }

		public int SourceCount {
			get { return tableSources.Count; }
		}

		public bool CompareStrings(string str1, string str2) {
			var compareType = IgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return String.Equals(str1, str2, compareType);
		}

		public void AddTable(IFromTableSource tableSource) {
			tableSources.Add(tableSource);
		}

		public void AddExpression(ExpressionReference expressionReference) {
			expressionReferences.Add(expressionReference);
		}

		public void ExposeColumn(ObjectName columName) {
			exposedColumns.Add(columName);
		}

		public void ExposeColumns(IFromTableSource tableSource) {
			foreach (var column in tableSource.ColumnNames) {
				exposedColumns.Add(column);
			}
		}

		public void ExposeColumns(ObjectName tableName) {
			var schema = tableName.Parent != null ? tableName.Parent.Name : null;
			IFromTableSource table = FindTable(schema, tableName.Name);
			if (table == null)
				throw new InvalidOperationException("Table name found: " + tableName);

			ExposeColumns(table);
		}

		public void ExposeAllColumns() {
			foreach (var source in tableSources) {
				ExposeColumns(source);
			}
		}

		public IFromTableSource GetTableSource(int offset) {
			return tableSources[offset];
		}

		public IFromTableSource FindTable(string schema, string name) {
			return tableSources.FirstOrDefault(x => x.MatchesReference(null, schema, name));
		}

		public ObjectName[] GetResolvedColumns() {
			var columnNames = new ObjectName[exposedColumns.Count];
			exposedColumns.CopyTo(columnNames, 0);
			return columnNames;
		}

		public SqlExpression FindExpression(ObjectName alias) {
			if (alias == null) 
				throw new ArgumentNullException("alias");

			if (alias.Parent != null)
				return null;

			var aliasName = alias.Name;

			int matchCount = 0;
			SqlExpression expToRetun = null;
			foreach (var reference in expressionReferences) {
				if (matchCount > 1)
					throw new AmbiguousMatchException(String.Format("Alias '{0}' resolves to multiple expressions in the plan", alias));

				if (CompareStrings(reference.Alias, aliasName)) {
					expToRetun = reference.Expression;
					matchCount++;
				}
			}

			return expToRetun;
		}

		private ObjectName ResolveColumnReference(ObjectName columnName) {
			var parent = columnName.Parent;
			var name = columnName.Name;
			string schemaName = null;
			string tableName = null;
			if (parent != null) {
				tableName = parent.Name;
				parent = parent.Parent;
				if (parent != null)
					schemaName = parent.Name;
			}

			ObjectName matchedColumn = null;

			foreach (var source in tableSources) {
				int matchCount = source.ResolveColumnCount(null, schemaName, tableName, name);
				if (matchCount > 1)
					throw new AmbiguousMatchException();

				if (matchCount == 1)
					matchedColumn = source.ResolveColumn(null, schemaName, tableName, name);
			}

			return matchedColumn;
		}

		private ObjectName ResolveAliasReference(ObjectName alias) {
			if (alias.Parent != null)
				return null;

			var aliasName = alias.Name;

			int matchCount = 0;
			ObjectName matched = null;
			foreach (var reference in expressionReferences) {
				if (matchCount > 1)
					throw new AmbiguousMatchException();

				if (CompareStrings(aliasName, reference.Alias)) {
					matched = new ObjectName(reference.Alias);
					matchCount++;
				}
			}

			return matched;
		}

		public ObjectName ResolveReference(ObjectName refName) {
			var refList = new List<ObjectName>();

			var resolved = ResolveAliasReference(refName);
			if (resolved != null)
				refList.Add(resolved);

			resolved = ResolveColumnReference(refName);
			if (resolved != null)
				refList.Add(resolved);

			if (refList.Count > 1)
				throw new AmbiguousMatchException();

			if (refList.Count == 0)
				return null;

			return refList[0];
		}

		private QueryReference GlobalResolveReference(int level, ObjectName name) {
			ObjectName resolvedName = ResolveReference(name);
			if (resolvedName == null && Parent != null)
				// If we need to descend to the parent, increment the level.
				return Parent.GlobalResolveReference(level + 1, name);

			if (resolvedName != null)
				return new QueryReference(resolvedName, level);

			return null;
		}

		private object QualifyReference(ObjectName name) {
			var referenceName = ResolveReference(name);
			if (referenceName == null) {
				if (Parent == null)
					throw new InvalidOperationException(String.Format("Reference {0} was not found in context.", name));

				var queryRef = GlobalResolveReference(1, name);
				if (queryRef == null)
					throw new InvalidOperationException(String.Format("Reference {0} was not found in context.", name));

				return queryRef;
			}

			return referenceName;
		}

		public static QueryExpressionFrom Create(IQueryContext context, SqlQueryExpression expression) {
			// Get the 'from_clause' from the table expression
			var fromClause = expression.FromClause;
			var ignoreCase = context.IgnoreIdentifiersCase();

			var queryFrom = new QueryExpressionFrom(ignoreCase);
			foreach (var fromTable in fromClause.AllTables) {
				var uniqueKey = fromTable.UniqueKey;
				var alias = fromTable.Alias;

				if (fromTable.IsSubQuery) {
					// eg. FROM ( SELECT id FROM Part )
					var subQuery = fromTable.SubQuery;
					var subQueryFrom = Create(context, subQuery);

					// The aliased name of the table
					ObjectName aliasTableName = null;
					if (alias != null)
						aliasTableName = new ObjectName(alias);

					// Add to list of sub-query tables to add to command,
					queryFrom.AddTable(new FromTableSubQuerySource(ignoreCase, uniqueKey, subQuery, subQueryFrom, aliasTableName));
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
					ITableQueryInfo tableQueryInfo = context.GetTableQueryInfo(tableName, givenName);

					queryFrom.AddTable(new FromTableDirectSource(ignoreCase, tableQueryInfo, uniqueKey, givenName, tableName));
				}
			}

			// Set up functions, aliases and exposed variables for this from set,

			foreach (var selectColumn in expression.SelectColumns) {
				// Is this a glob?  (eg. Part.* )
				if (selectColumn.IsGlob) {
					// Find the columns globbed and add to the 'selectedColumns' result.
					if (selectColumn.IsAll) {
						queryFrom.ExposeAllColumns();
					} else {
						// Otherwise the glob must be of the form '[table name].*'
						queryFrom.ExposeColumns(selectColumn.TableName);
					}
				} else {
					// Otherwise must be a standard column reference.  Note that at this
					// time we aren't sure if a column expression is correlated and is
					// referencing an outer source.  This means we can't verify if the
					// column expression is valid or not at this point.

					// If this column is aliased, add it as a function reference to the
					// select expression

					string alias = selectColumn.Alias;
					var v = selectColumn.Expression.AsReferenceName();
					bool aliasMatchV = (v != null && alias != null &&
										queryFrom.CompareStrings(v.Name, alias));
					if (alias != null && !aliasMatchV) {
						queryFrom.AddExpression(new ExpressionReference(selectColumn.Expression, alias));
						queryFrom.ExposeColumn(new ObjectName(alias));
					} else if (v != null) {
						var resolved = queryFrom.ResolveReference(v);
						queryFrom.ExposeColumn(resolved ?? v);
					} else {
						string funName = selectColumn.Expression.ToString();
						queryFrom.AddExpression(new ExpressionReference(selectColumn.Expression, funName));
						queryFrom.ExposeColumn(new ObjectName(funName));
					}
				}
			}

			return queryFrom;
		}

		#region FromExpressionPreparer

		private class FromExpressionPreparer : IExpressionPreparer {
			private readonly QueryExpressionFrom fromSet;

			public FromExpressionPreparer(QueryExpressionFrom fromSet) {
				this.fromSet = fromSet;
			}

			public bool CanPrepare(SqlExpression expression) {
				return expression is SqlReferenceExpression;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				var refName = ((SqlReferenceExpression) expression).ReferenceName;

				var reference = fromSet.QualifyReference(refName);
				if (reference is ObjectName)
					return SqlExpression.Reference((ObjectName) reference);
				if (reference is QueryReference)
					return new QueryReferenceExpression((QueryReference) reference);

				throw new InvalidOperationException();
			}
		}

		#endregion
	}
}