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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="TableName"/>/<see cref="DataTableBase"/> object.
	/// </summary>
	/// <remarks>
	/// The handles case insensitive resolution.
	/// </remarks>
	public class FromTableDirectSource : IFromTableSource {
		/// <summary>
		/// The ITableQueryDef object that links to the underlying table.
		/// </summary>
		private readonly ITableQueryDef table_query;

		/// <summary>
		/// The DataTableDef object that describes the table.
		/// </summary>
		private readonly DataTableDef data_table_def;

		/// <summary>
		/// The unique name given to this source.
		/// </summary>
		private readonly String unique_name;

		/// <summary>
		/// The given TableName of this table.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// The root name of the table.
		/// </summary>
		/// <remarks>
		/// For example, if this table is <i>Part P</i> the root name 
		/// is <i>Part</i> and <i>P</i> is the aliased name.
		/// </remarks>
		private readonly TableName root_name;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool case_insensitive = false;

		/// <summary>
		/// Constructs the source.
		/// </summary>
		/// <param name="case_insensitive"></param>
		/// <param name="table_query"></param>
		/// <param name="unique_name"></param>
		/// <param name="given_name"></param>
		/// <param name="root_name"></param>
		public FromTableDirectSource(bool case_insensitive,
		                             ITableQueryDef table_query, String unique_name,
		                             TableName given_name, TableName root_name) {
			this.unique_name = unique_name;
			this.data_table_def = table_query.DataTableDef;
			this.root_name = root_name;
			if (given_name != null) {
				this.table_name = given_name;
			} else {
				this.table_name = root_name;
			}
			// Is the database case insensitive?
			this.case_insensitive = case_insensitive;
			this.table_query = table_query;
		}

		/// <summary>
		/// Gets the given name of the table, if set, otherwise gets the
		/// root table name.
		/// </summary>
		/// <remarks>
		/// For example, if the Part table is aliased as P this returns P.
		/// </remarks>
		public TableName GivenTableName {
			get { return table_name; }
		}

		/// <summary>
		/// Gets the root name of the table.
		/// </summary>
		/// <remarks>
		/// This name can always be used as a direct reference to a 
		/// table in the database.
		/// </remarks>
		public TableName RootTableName {
			get { return root_name; }
		}

		/// <summary>
		/// Creates a query plan node to be added into a query tree that 
		/// fetches the table source.
		/// </summary>
		/// <returns></returns>
		public IQueryPlanNode CreateFetchQueryPlanNode() {
			return table_query.QueryPlanNode;
		}

		///<summary>
		/// Toggle the case sensitivity flag.
		///</summary>
		///<param name="status"></param>
		public void SetCaseInsensitive(bool status) {
			case_insensitive = status;
		}

		private bool StringCompare(String str1, String str2) {
			if (!case_insensitive) {
				return str1.Equals(str2);
			}
			return String.Compare(str1, str2, true) == 0;
		}


		// ---------- Implemented from IFromTableSource ----------

		public string UniqueName {
			get { return unique_name; }
		}

		public bool MatchesReference(string catalog, string schema, string table) {
			//    Console.Out.WriteLine("Matches reference: " + schema + " " + table);
			//    Console.Out.WriteLine(table_name.getName());

			// Does this table name represent the correct schema?
			if (schema != null &&
			    !StringCompare(schema, table_name.Schema)) {
				// If schema is present and we can't resolve to this schema then false
				return false;
			}
			if (table != null &&
			    !StringCompare(table, table_name.Name)) {
				// If table name is present and we can't resolve to this table name
				// then return false
				return false;
			}
			//    Console.Out.WriteLine("MATCHED!");
			// Match was successful,
			return true;
		}

		public int ResolveColumnCount(String catalog, String schema,
		                              String table, String column) {
			// NOTE: With this type, we can only ever return either 1 or 0 because
			//   it's impossible to have an ambiguous reference

			// NOTE: Currently 'catalog' is ignored.

			// Does this table name represent the correct schema?
			if (schema != null &&
			    !StringCompare(schema, table_name.Schema)) {
				// If schema is present and we can't resolve to this schema then return 0
				return 0;
			}
			if (table != null &&
			    !StringCompare(table, table_name.Name)) {
				// If table name is present and we can't resolve to this table name then
				// return 0
				return 0;
			}

			if (column != null) {
				if (!case_insensitive) {
					// Can we resolve the column in this table?
					int i = data_table_def.FastFindColumnName(column);
					// If i doesn't equal -1 then we've found our column
					return i == -1 ? 0 : 1;
				} else {
					// Case insensitive search (this is slower than case sensitive).
					int resolve_count = 0;
					int col_count = data_table_def.ColumnCount;
					for (int i = 0; i < col_count; ++i) {
						if (String.Compare(data_table_def[i].Name, column, true) == 0) {
							++resolve_count;
						}
					}
					return resolve_count;
				}
			} else {  // if (column == null)
				// Return the column count
				return data_table_def.ColumnCount;
			}
		}

		public VariableName ResolveColumn(String catalog, String schema,
		                                  String table, String column) {

			// Does this table name represent the correct schema?
			if (schema != null &&
			    !StringCompare(schema, table_name.Schema)) {
				// If schema is present and we can't resolve to this schema
				throw new ApplicationException("Incorrect schema.");
			}
			if (table != null &&
			    !StringCompare(table, table_name.Name)) {
				// If table name is present and we can't resolve to this table name
				throw new ApplicationException("Incorrect table.");
			}

			if (column != null) {
				if (!case_insensitive) {
					// Can we resolve the column in this table?
					int i = data_table_def.FastFindColumnName(column);
					if (i == -1) {
						throw new ApplicationException("Could not resolve '" + column + "'");
					}
					return new VariableName(table_name, column);
				} else {
					// Case insensitive search (this is slower than case sensitive).
					int col_count = data_table_def.ColumnCount;
					for (int i = 0; i < col_count; ++i) {
						String col_name = data_table_def[i].Name;
						if (String.Compare(col_name, column, true) == 0) {
							return new VariableName(table_name, col_name);
						}
					}
					throw new ApplicationException("Could not resolve '" + column + "'");
				}
			} else {  // if (column == null)
				// Return the first column in the table
				return new VariableName(table_name, data_table_def[0].Name);
			}

		}

		public VariableName[] AllColumns {
			get {
				int col_count = data_table_def.ColumnCount;
				VariableName[] vars = new VariableName[col_count];
				for (int i = 0; i < col_count; ++i) {
					vars[i] = new VariableName(table_name, data_table_def[i].Name);
				}
				return vars;
			}
		}
	}
}