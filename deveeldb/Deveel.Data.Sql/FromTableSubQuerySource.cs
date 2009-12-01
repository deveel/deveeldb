//  
//  FromTableSubQuerySource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="TableSelectExpression"/> as a sub-query source.
	/// </summary>
	public class FromTableSubQuerySource : IFromTableSource {
		/// <summary>
		/// The wrapped object.
		/// </summary>
		private readonly TableSelectExpression table_expression;

		/// <summary>
		/// The fully prepared <see cref="TableExpressionFromSet"/> object 
		/// that is used to qualify variables in the table.
		/// </summary>
		private readonly TableExpressionFromSet from_set;

		/// <summary>
		/// The TableName that this source is generated to (aliased name).
		/// </summary>
		/// <remarks>
		/// If null, we inherit from the root set.
		/// </remarks>
		private readonly TableName end_table_name;

		/// <summary>
		/// A unique name given to this source that is used to reference it in a table-set.
		/// </summary>
		private readonly String unique_key;

		/// <summary>
		/// The list of all variable names in the resultant source.
		/// </summary>
		private VariableName[] vars;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool case_insensitive = false;

		/// <summary>
		/// Constructs the source.
		/// </summary>
		/// <param name="case_insensitive"></param>
		/// <param name="unique_key"></param>
		/// <param name="table_expression"></param>
		/// <param name="from_set"></param>
		/// <param name="aliased_table_name"></param>
		internal FromTableSubQuerySource(bool case_insensitive,
									   String unique_key,
									   TableSelectExpression table_expression,
									   TableExpressionFromSet from_set,
									   TableName aliased_table_name) {
			this.unique_key = unique_key;
			this.table_expression = table_expression;
			this.from_set = from_set;
			this.end_table_name = aliased_table_name;
			// Is the database case insensitive?
			this.case_insensitive = case_insensitive;
		}

		/// <summary>
		/// Gets the <see cref="TableSelectExpression"/> the sub-query is 
		/// wrapping around.
		/// </summary>
		internal TableSelectExpression TableExpression {
			get { return table_expression; }
		}

		/// <summary>
		/// Returns the <see cref="TableExpressionFromSet"/> for this sub-query.
		/// </summary>
		internal TableExpressionFromSet FromSet {
			get { return from_set; }
		}

		/// <summary>
		/// Returns the aliased table name of the sub-query or null if 
		/// it is left as-is.
		/// </summary>
		internal TableName AliasedName {
			get { return end_table_name; }
		}


		/// <summary>
		/// Makes sure the <see cref="vars"/> list is created correctly.
		/// </summary>
		private void EnsureVarList() {
			if (vars == null) {
				vars = from_set.GenerateResolvedVariableList();
				//      for (int i = 0; i < vars.length; ++i) {
				//        Console.Out.WriteLine("+ " + vars[i]);
				//      }
				//      Console.Out.WriteLine("0000");
				// Are the variables aliased to a table name?
				if (end_table_name != null) {
					for (int i = 0; i < vars.Length; ++i) {
						vars[i].TableName = end_table_name;
					}
				}
			}
		}

		/// <summary>
		/// Returns the unique name of this source.
		/// </summary>
		public string UniqueKey {
			get { return unique_key; }
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

		/// <summary>
		/// If the given Variable matches the reference then this method returns true.
		/// </summary>
		/// <param name="v"></param>
		/// <param name="catalog"></param>
		/// <param name="schema"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <returns></returns>
		private bool MatchesVar(VariableName v, String catalog, String schema,
											   String table, String column) {
			TableName tn = v.TableName;
			String cn = v.Name;

			if (column == null) {
				return true;
			}
			if (!StringCompare(cn, column)) {
				return false;
			}

			if (table == null) {
				return true;
			}
			if (tn == null) {
				return false;
			}
			String tname = tn.Name;
			if (tname != null && !StringCompare(tname, table)) {
				return false;
			}

			if (schema == null) {
				return true;
			}
			String sname = tn.Schema;
			if (sname != null && !StringCompare(sname, schema)) {
				return false;
			}

			// Currently we ignore catalog
			return true;

		}

		// ---------- Implemented from IFromTableSource ----------

		public string UniqueName {
			get { return UniqueKey; }
		}

		public bool MatchesReference(String catalog,
										String schema, String table) {
			if (schema == null && table == null) {
				return true;
			}
			if (end_table_name != null) {
				String ts = end_table_name.Schema;
				String tt = end_table_name.Name;
				if (schema == null) {
					if (StringCompare(tt, table)) {
						return true;
					}
				} else {
					if (StringCompare(tt, table) && StringCompare(ts, schema)) {
						return true;
					}
				}
			}
			// No way to determine if there is a match
			return false;
		}

		public int ResolveColumnCount(String catalog, String schema,
									  String table, String column) {
			EnsureVarList();

			if (catalog == null && schema == null && table == null && column == null) {
				// Return the column count
				return vars.Length;
			}

			int matched_count = 0;
			for (int i = 0; i < vars.Length; ++i) {
				VariableName v = vars[i];
				if (MatchesVar(v, catalog, schema, table, column)) {
					++matched_count;
				}
			}

			return matched_count;

		}

		public VariableName ResolveColumn(String catalog, String schema,
									  String table, String column) {
			EnsureVarList();

			//    Console.Out.WriteLine("ResolveColumn: " + catalog + ", " + schema + ", " +
			//                       table + ", " + column);

			for (int i = 0; i < vars.Length; ++i) {
				VariableName v = vars[i];
				if (MatchesVar(v, catalog, schema, table, column)) {
					//        Console.Out.WriteLine("Result: " + v);
					return v;
				}
			}

			throw new ApplicationException("Couldn't resolve to a column.");
		}

		public VariableName[] AllColumns {
			get {
				EnsureVarList();
				return vars;
			}
		}
	}
}