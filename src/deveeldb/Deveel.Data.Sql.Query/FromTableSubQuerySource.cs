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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
		/// <summary>
	/// An implementation of <see cref="IFromTableSource"/> that wraps around a 
	/// <see cref="SqlQueryExpression"/> as a sub-query source.
	/// </summary>
	public class FromTableSubQuerySource : IFromTableSource {
		/// <summary>
		/// The wrapped object.
		/// </summary>
		private readonly SqlQueryExpression tableExpression;

		/// <summary>
		/// The fully prepared <see cref="TableExpressionFromSet"/> object 
		/// that is used to qualify variables in the table.
		/// </summary>
		private readonly QueryExpressionFrom fromSet;

		/// <summary>
		/// The TableName that this source is generated to (aliased name).
		/// </summary>
		/// <remarks>
		/// If null, we inherit from the root set.
		/// </remarks>
		private readonly ObjectName endTableName;

		/// <summary>
		/// A unique name given to this source that is used to reference it in a table-set.
		/// </summary>
		private readonly string uniqueKey;

		/// <summary>
		/// The list of all variable names in the resultant source.
		/// </summary>
		private ObjectName[] vars;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool caseInsensitive;

		/// <summary>
		/// Constructs the source.
		/// </summary>
		/// <param name="caseInsensitive"></param>
		/// <param name="uniqueKey"></param>
		/// <param name="tableExpression"></param>
		/// <param name="fromSet"></param>
		/// <param name="aliasedTableName"></param>
		internal FromTableSubQuerySource(bool caseInsensitive, string uniqueKey, SqlQueryExpression tableExpression, 
			QueryExpressionFrom fromSet, ObjectName aliasedTableName) {
			this.uniqueKey = uniqueKey;
			this.tableExpression = tableExpression;
			this.fromSet = fromSet;
			endTableName = aliasedTableName;
			// Is the database case insensitive?
			this.caseInsensitive = caseInsensitive;
		}

		/// <summary>
		/// Makes sure the <see cref="vars"/> list is created correctly.
		/// </summary>
		private void EnsureVarList() {
			if (vars == null) {
				vars = fromSet.GetResolvedColumns();
				// Are the variables aliased to a table name?
				if (endTableName != null) {
					for (int i = 0; i < vars.Length; ++i) {
						vars[i] = new ObjectName(endTableName, vars[i].Name);
					}
				}
			}
		}

		/// <summary>
		/// Returns the unique name of this source.
		/// </summary>
		public string UniqueKey {
			get { return uniqueKey; }
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

		/// <summary>
		/// If the given Variable matches the reference then this method returns true.
		/// </summary>
		/// <param name="v"></param>
		/// <param name="catalog"></param>
		/// <param name="schema"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <returns></returns>
		private bool MatchesVar(ObjectName v, string catalog, string schema, string table, string column) {
			var tn = v.Parent;
			String cn = v.Name;

			if (column == null)
				return true;
			if (!StringCompare(cn, column))
				return false;

			if (table == null)
				return true;
			if (tn == null)
				return false;

			string tname = tn.Name;
			if (tname != null && !StringCompare(tname, table))
				return false;

			if (schema == null)
				return true;

			string sname = tn.Parent != null ? tn.Parent.Name : null;
			if (sname != null && !StringCompare(sname, schema))
				return false;

			// Currently we ignore catalog
			return true;
		}

		// ---------- Implemented from IFromTableSource ----------

		public string UniqueName {
			get { return UniqueKey; }
		}

		public bool MatchesReference(string catalog, string schema, string table) {
			if (schema == null && table == null)
				return true;

			if (endTableName != null) {
				string ts = endTableName.Parent.Name;
				string tt = endTableName.Name;
				if (schema == null)
					return StringCompare(tt, table);
				if (StringCompare(tt, table) && StringCompare(ts, schema))
					return true;
			}

			// No way to determine if there is a match
			return false;
		}

		public int ResolveColumnCount(string catalog, string schema, string table, string column) {
			EnsureVarList();

			if (catalog == null && schema == null && table == null && column == null) {
				// Return the column count
				return vars.Length;
			}

			int matchedCount = 0;
			foreach (var v in vars) {
				if (MatchesVar(v, catalog, schema, table, column))
					++matchedCount;
			}

			return matchedCount;

		}

		public ObjectName ResolveColumn(string catalog, string schema, string table, string column) {
			EnsureVarList();

			foreach (var v in vars) {
				if (MatchesVar(v, catalog, schema, table, column)) {
					return v;
				}
			}

			throw new ApplicationException("Couldn't resolve to a column.");
		}

		public ObjectName[] AllColumns {
			get {
				EnsureVarList();
				return vars;
			}
		}
	}
}