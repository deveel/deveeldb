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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="TableName"/>/<see cref="DataTableBase"/> object.
	/// </summary>
	/// <remarks>
	/// The handles case insensitive resolution.
	/// </remarks>
	public class FromTableDirectSource : IFromTableSource {
		/// <summary>
		/// The ITableQueryInfo object that links to the underlying table.
		/// </summary>
		private readonly ITableQueryInfo tableQuery;

		/// <summary>
		/// The DataTableInfo object that describes the table.
		/// </summary>
		private readonly DataTableInfo dataTableInfo;

		/// <summary>
		/// The unique name given to this source.
		/// </summary>
		private readonly string uniqueName;

		/// <summary>
		/// The given TableName of this table.
		/// </summary>
		private readonly TableName tableName;

		/// <summary>
		/// The root name of the table.
		/// </summary>
		/// <remarks>
		/// For example, if this table is <i>Part P</i> the root name 
		/// is <i>Part</i> and <i>P</i> is the aliased name.
		/// </remarks>
		private readonly TableName rootName;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool caseInsensitive;

		/// <summary>
		/// Constructs the source.
		/// </summary>
		/// <param name="caseInsensitive"></param>
		/// <param name="tableQuery"></param>
		/// <param name="uniqueName"></param>
		/// <param name="givenName"></param>
		/// <param name="rootName"></param>
		public FromTableDirectSource(bool caseInsensitive, ITableQueryInfo tableQuery, string uniqueName, TableName givenName, TableName rootName) {
			this.uniqueName = uniqueName;
			dataTableInfo = tableQuery.TableInfo;
			this.rootName = rootName;
			if (givenName != null) {
				tableName = givenName;
			} else {
				tableName = rootName;
			}

			// Is the database case insensitive?
			this.caseInsensitive = caseInsensitive;
			this.tableQuery = tableQuery;
		}

		/// <summary>
		/// Gets the given name of the table, if set, otherwise gets the
		/// root table name.
		/// </summary>
		/// <remarks>
		/// For example, if the Part table is aliased as P this returns P.
		/// </remarks>
		public TableName GivenTableName {
			get { return tableName; }
		}

		/// <summary>
		/// Gets the root name of the table.
		/// </summary>
		/// <remarks>
		/// This name can always be used as a direct reference to a 
		/// table in the database.
		/// </remarks>
		public TableName RootTableName {
			get { return rootName; }
		}

		/// <summary>
		/// Creates a query plan node to be added into a query tree that 
		/// fetches the table source.
		/// </summary>
		/// <returns></returns>
		public IQueryPlanNode CreateFetchQueryPlanNode() {
			return tableQuery.QueryPlanNode;
		}

		///<summary>
		/// Toggle the case sensitivity flag.
		///</summary>
		///<param name="status"></param>
		public void SetCaseInsensitive(bool status) {
			caseInsensitive = status;
		}

		private bool StringCompare(string str1, string str2) {
			return String.Compare(str1, str2, caseInsensitive) == 0;
		}


		// ---------- Implemented from IFromTableSource ----------

		public string UniqueName {
			get { return uniqueName; }
		}

		public bool MatchesReference(string catalog, string schema, string table) {
			// Does this table name represent the correct schema?
			if (schema != null &&
			    !StringCompare(schema, tableName.Schema)) {
				// If schema is present and we can't resolve to this schema then false
				return false;
			}
			if (table != null &&
			    !StringCompare(table, tableName.Name)) {
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

			// NOTE: Currently 'catalog' is ignored.

			// Does this table name represent the correct schema?
			if (schema != null && !StringCompare(schema, tableName.Schema)) {
				// If schema is present and we can't resolve to this schema then return 0
				return 0;
			}
			if (table != null && !StringCompare(table, tableName.Name)) {
				// If table name is present and we can't resolve to this table name then
				// return 0
				return 0;
			}

			if (column != null) {
				if (!caseInsensitive) {
					// Can we resolve the column in this table?
					int i = dataTableInfo.FastFindColumnName(column);
					// If i doesn't equal -1 then we've found our column
					return i == -1 ? 0 : 1;
				}

				// Case insensitive search (this is slower than case sensitive).
				int resolveCount = 0;
				int colCount = dataTableInfo.ColumnCount;
				for (int i = 0; i < colCount; ++i) {
					if (String.Compare(dataTableInfo[i].Name, column, true) == 0)
						++resolveCount;
				}
				return resolveCount;
			} // if (column != null)

			// Return the column count
			return dataTableInfo.ColumnCount;
		}

		public VariableName ResolveColumn(string catalog, string schema, string table, string column) {
			// Does this table name represent the correct schema?
			if (schema != null && !StringCompare(schema, tableName.Schema))
				// If schema is present and we can't resolve to this schema
				throw new ApplicationException("Incorrect schema.");

			if (table != null && !StringCompare(table, tableName.Name))
				// If table name is present and we can't resolve to this table name
				throw new ApplicationException("Incorrect table.");

			if (column != null) {
				if (!caseInsensitive) {
					// Can we resolve the column in this table?
					int i = dataTableInfo.FastFindColumnName(column);
					if (i == -1)
						throw new ApplicationException("Could not resolve '" + column + "'");

					return new VariableName(tableName, column);
				}

				// Case insensitive search (this is slower than case sensitive).
				int colCount = dataTableInfo.ColumnCount;
				for (int i = 0; i < colCount; ++i) {
					string colName = dataTableInfo[i].Name;
					if (String.Compare(colName, column, true) == 0) {
						return new VariableName(tableName, colName);
					}
				}
				throw new ApplicationException("Could not resolve '" + column + "'");
			} // if (column != null)

			// Return the first column in the table
			return new VariableName(tableName, dataTableInfo[0].Name);
		}

		public VariableName[] AllColumns {
			get {
				int colCount = dataTableInfo.ColumnCount;
				VariableName[] vars = new VariableName[colCount];
				for (int i = 0; i < colCount; ++i) {
					vars[i] = new VariableName(tableName, dataTableInfo[i].Name);
				}
				return vars;
			}
		}
	}
}