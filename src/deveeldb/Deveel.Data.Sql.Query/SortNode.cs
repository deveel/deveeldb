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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	class SortNode : SingleQueryPlanNode {
		public SortNode(IQueryPlanNode child, ObjectName[] columnNames, bool[] ascending) 
			: base(child) {

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

			ColumnNames = columnNames;
			Ascending = ascending;
		}

		public ObjectName[] ColumnNames { get; private set; }

		public bool[] Ascending { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var t = Child.Evaluate(context);
			return t.OrderBy(ColumnNames, Ascending);
		}
	}
}
