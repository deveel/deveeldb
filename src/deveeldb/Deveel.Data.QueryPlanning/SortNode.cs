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
using System.Text;

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// The node for performing a sort operation on the given columns of 
	/// the child node.
	/// </summary>
	[Serializable]
	public class SortNode : SingleQueryPlanNode {
		/// <summary>
		/// The list of columns to sort.
		/// </summary>
		private readonly VariableName[] columns;

		/// <summary>
		/// Whether to sort the column in ascending or descending order
		/// </summary>
		private readonly bool[] ascending;

		public SortNode(IQueryPlanNode child, VariableName[] columns, bool[] ascending)
			: base(child) {
			this.columns = columns;
			this.ascending = ascending;

			// How we handle ascending/descending order
			// ----------------------------------------
			// Internally to the database, all columns are naturally ordered in
			// ascending order (start at lowest and end on highest).  When a column
			// is ordered in descending order, a fast way to achieve this is to take
			// the ascending set and reverse it.  This works for single columns,
			// however some thought is required for handling multiple column.  We
			// order columns from RHS to LHS.  If LHS is descending then this will
			// order the RHS incorrectly if we leave as is.  Therefore, we must do
			// some pre-processing that looks ahead on any descending orders and
			// reverses the order of the columns to the right.  This pre-processing
			// is done in the first pass.

			int sz = ascending.Length;
			for (int n = 0; n < sz - 1; ++n) {
				if (!ascending[n]) {    // if descending...
					// Reverse order of all columns to the right...
					for (int p = n + 1; p < sz; ++p) {
						ascending[p] = !ascending[p];
					}
				}
			}

		}

		public override Table Evaluate(IQueryContext context) {
			Table t = Child.Evaluate(context);
			// Sort the results by the columns in reverse-safe order.
			int sz = ascending.Length;
			for (int n = sz - 1; n >= 0; --n) {
				t = t.OrderByColumn(columns[n], ascending[n]);
			}
			return t;
		}

		public override Object Clone() {
			SortNode node = (SortNode)base.Clone();
			QueryPlanUtil.CloneArray(node.columns);
			return node;
		}

		public override string Title {
			get {
				StringBuilder sb = new StringBuilder();
				sb.Append("SORT: (");
				for (int i = 0; i < columns.Length; ++i) {
					sb.Append(columns[i]);
					if (ascending[i]) {
						sb.Append(" ASC");
					} else {
						sb.Append(" DESC");
					}
					
					if (i <  columns.Length - 1)
						sb.Append(", ");
				}
				sb.Append(")");
				return sb.ToString();
			}
		}
	}
}