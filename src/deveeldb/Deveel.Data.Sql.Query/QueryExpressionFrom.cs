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
using System.Runtime.Remoting.Messaging;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryExpressionFrom {
		private readonly List<IFromTableSource> tableSources;
		private readonly List<ExpressionReference> expressionReferences;
		private readonly List<ObjectName> exposedColumns; 

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
			foreach (var column in tableSource.AllColumns) {
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

		private class FromExpressionPreparer : IExpressionPreparer {
			private readonly QueryExpressionFrom fromSet;

			public FromExpressionPreparer(QueryExpressionFrom fromSet) {
				this.fromSet = fromSet;
			}

			public bool CanPrepare(object element) {
				return element is ObjectName;
			}

			public SqlExpression Prepare(object element) {
				var name = (ObjectName) element;
				var reference = fromSet.QualifyReference(name);
				if (reference is ObjectName)
					return SqlExpression.Reference((ObjectName) reference);

				return new QueryReferenceExpression((QueryReference) reference);
			}
		}
	}
}