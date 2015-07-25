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
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="SqlQueryExpression"/> as a sub-query source.
	/// </summary>
	public class FromTableSubQuerySource : IFromTableSource {
		private ObjectName[] columnNames;

		internal FromTableSubQuerySource(bool caseInsensitive, string uniqueKey, SqlQueryExpression queryExpression,
			QueryExpressionFrom fromSet, ObjectName alias) {
			UniqueName = uniqueKey;
			QueryExpression = queryExpression;
			QueryFrom = fromSet;
			AliasName = alias;
			IgnoreCase = caseInsensitive;
		}

		public bool MatchesReference(string catalog, string schema, string table) {
			if (schema == null && table == null)
				return true;

			if (AliasName != null) {
				string ts = AliasName.Parent.Name;
				string tt = AliasName.Name;
				if (schema == null)
					return StringCompare(tt, table);
				if (StringCompare(tt, table) && StringCompare(ts, schema))
					return true;
			}

			// No way to determine if there is a match
			return false;
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public QueryExpressionFrom QueryFrom { get; private set; }

		public ObjectName AliasName { get; private set; }

		public bool IgnoreCase { get; private set; }

		public string UniqueName { get; private set; }

		public ObjectName[] ColumnNames {
			get {
				EnsureColumnNames();
				return columnNames;
			}
		}

		/// <summary>
		/// Makes sure the <see cref="columnNames"/> list is created correctly.
		/// </summary>
		private void EnsureColumnNames() {
			if (columnNames == null) {
				columnNames = QueryFrom.GetResolvedColumns();

				// Are the variables aliased to a table name?
				if (AliasName != null) {
					for (int i = 0; i < columnNames.Length; ++i) {
						columnNames[i] = new ObjectName(AliasName, columnNames[i].Name);
					}
				}
			}
		}

		private bool StringCompare(string str1, string str2) {
			var comparison = IgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return String.Equals(str1, str2, comparison);
		}

		private bool Matches(ObjectName name, string catalog, string schema, string table, string column) {
			var tableName = name.Parent;
			var schemaName = tableName == null ? null : tableName.Parent;
			var catalogName = schemaName == null ? null : schemaName.Parent;
			var columnName = name.Name;

			if (column == null)
				return true;
			if (!StringCompare(columnName, column))
				return false;

			if (table == null)
				return true;
			if (tableName == null)
				return false;

			string tname = tableName.Name;
			if (tname != null && !StringCompare(tname, table))
				return false;

			if (schema == null)
				return true;

			string sname = schemaName != null ? schemaName.Name : null;
			if (sname != null && !StringCompare(sname, schema))
				return false;

			string cname = catalogName != null ? catalogName.Name : null;
			if (cname != null && !StringCompare(cname, catalog))
				return false;

			return true;
		}

		public int ResolveColumnCount(string catalog, string schema, string table, string column) {
			EnsureColumnNames();

			if (String.IsNullOrEmpty(catalog) && 
				String.IsNullOrEmpty(schema) && 
				String.IsNullOrEmpty(table) && 
				String.IsNullOrEmpty(column))
				return columnNames.Length;

			return columnNames.Count(v => Matches(v, catalog, schema, table, column));

		}

		public ObjectName ResolveColumn(string catalog, string schema, string table, string column) {
			EnsureColumnNames();

			var result = columnNames.FirstOrDefault(v => Matches(v, catalog, schema, table, column));
			if (result == null)
				throw new InvalidOperationException("Couldn't resolve to a column.");

			return result;
		}
	}
}