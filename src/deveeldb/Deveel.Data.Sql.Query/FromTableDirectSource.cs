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

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="ObjectName"/>/<see cref="ITable"/> object.
	/// </summary>
	/// <remarks>
	/// The handles case insensitive resolution.
	/// </remarks>
	public class FromTableDirectSource : IFromTableSource {
		private readonly ITableQueryInfo tableQuery;
		private readonly TableInfo tableInfo;

		/// <summary>
		/// Constructs the source.
		/// </summary>
		/// <param name="caseInsensitive"></param>
		/// <param name="tableQuery"></param>
		/// <param name="uniqueName"></param>
		/// <param name="givenName"></param>
		/// <param name="rootName"></param>
		public FromTableDirectSource(bool caseInsensitive, ITableQueryInfo tableQuery, string uniqueName, ObjectName givenName, ObjectName rootName) {
			this.UniqueName = uniqueName;
			tableInfo = tableQuery.TableInfo;
			RootTableName = rootName;
			if (givenName != null) {
				GivenTableName = givenName;
			} else {
				GivenTableName = rootName;
			}

			IgnoreCase = caseInsensitive;
			this.tableQuery = tableQuery;
		}

		public bool IgnoreCase { get; private set; }

		public ObjectName GivenTableName { get; private set; }

		public ObjectName RootTableName { get; private set; }

		public IQueryPlanNode QueryPlan {
			get { return tableQuery.QueryPlanNode; }
		}

		public string UniqueName { get; private set; }

		public ObjectName[] ColumnNames {
			get {
				int colCount = tableInfo.ColumnCount;
				var vars = new ObjectName[colCount];
				for (int i = 0; i < colCount; ++i) {
					vars[i] = new ObjectName(GivenTableName, tableInfo[i].ColumnName);
				}
				return vars;
			}
		}

		private bool StringCompare(string str1, string str2) {
			var comparison = IgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return String.Equals(str1, str2, comparison);
		}


		public bool MatchesReference(string catalog, string schema, string table) {
			var schemaName = GivenTableName.Parent;
			var catalogName = schemaName == null ? null : schemaName.Parent;

			// Does this table name represent the correct schema?
			var givenSchema = schemaName != null ? schemaName.Name : null;
			if (schema != null && !StringCompare(schema, givenSchema)) {
				// If schema is present and we can't resolve to this schema then false
				return false;
			}

			var givenCatalog = catalogName != null ? catalogName.Name : null;
			if (catalog != null && !StringCompare(catalog, givenCatalog))
				return false;

			if (table != null && !StringCompare(table, GivenTableName.Name)) {
				// If table name is present and we can't resolve to this table name
				// then return false
				return false;
			}

			// Match was successful,
			return true;
		}

		public int ResolveColumnCount(string catalog, string schema, string table, string column) {
			// NOTE: With this type, we can only ever return either 1 or 0 because
			//   it's impossible to have an ambiguous reference

			var schemaName = GivenTableName.Parent;
			var catalogName = schemaName == null ? null : schemaName.Parent;

			var givenCatalog = catalogName != null ? catalogName.Name : null;
			if (catalog != null && !StringCompare(catalog, givenCatalog))
				return 0;

			var givenSchema = schemaName != null ? schemaName.Name : null;
			if (schema != null && !StringCompare(schema, givenSchema))
				return 0;

			if (table != null && !StringCompare(table, GivenTableName.Name)) {
				return 0;
			}

			if (column != null) {
				// TODO: the case-insensitive search in TableInfo
				if (!IgnoreCase) {
					// Can we resolve the column in this table?
					int i = tableInfo.IndexOfColumn(column);
					// If i doesn't equal -1 then we've found our column
					return i == -1 ? 0 : 1;
				}

				return tableInfo.Count(columnInfo => StringCompare(columnInfo.ColumnName, column));
			}

			// Return the column count
			return tableInfo.ColumnCount;
		}

		public ObjectName ResolveColumn(string catalog, string schema, string table, string column) {
			var schemaName = GivenTableName.Parent;
			var catalogName = schemaName == null ? null : schemaName.Parent;

			var givenCatalog = catalogName != null ? catalogName.Name : null;
			if (catalog != null && !StringCompare(catalog, givenCatalog))
				throw new ApplicationException("Incorrect catalog.");

			// Does this table name represent the correct schema?
			var givenSchema = GivenTableName.Parent != null ? GivenTableName.Parent.Name : null;
			if (schema != null && !StringCompare(schema, givenSchema))
				// If schema is present and we can't resolve to this schema
				throw new ApplicationException("Incorrect schema.");

			if (table != null && !StringCompare(table, GivenTableName.Name))
				// If table name is present and we can't resolve to this table name
				throw new ApplicationException("Incorrect table.");

			if (column != null) {
				if (!IgnoreCase) {
					// Can we resolve the column in this table?
					int i = tableInfo.IndexOfColumn(column);
					if (i == -1)
						throw new ApplicationException("Could not resolve '" + column + "'");

					return new ObjectName(GivenTableName, column);
				}

				// Case insensitive search (this is slower than case sensitive).
				var columnName =
					tableInfo.Where(x => StringCompare(x.ColumnName, column))
						.Select(x => x.ColumnName)
						.FirstOrDefault();

				if (String.IsNullOrEmpty(columnName))
					throw new ApplicationException(String.Format("Could not resolve column '{0}' within the table '{1}'.", column,
						GivenTableName));

				return new ObjectName(GivenTableName, columnName);
			}

			// Return the first column in the table
			return new ObjectName(GivenTableName, tableInfo[0].ColumnName);
		}
	}
}