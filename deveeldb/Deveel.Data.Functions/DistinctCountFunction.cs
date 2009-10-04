//  
//  DistinctCountFunction.cs
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
using System.Collections;

namespace Deveel.Data.Functions {
	internal sealed class DistinctCountFunction : Function {
		public DistinctCountFunction(Expression[] parameters)
			: base("distinct_count", parameters) {
			SetAggregate(true);

			if (ParameterCount <= 0) {
				throw new Exception("'distinct_count' function must have at least one argument.");
			}

		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
			// There's some issues with implementing this function.
			// For this function to be efficient, we need to have access to the
			// underlying Table object(s) so we can use table indexing to sort the
			// columns.  Otherwise, we will need to keep in memory the group
			// contents so it can be sorted.  Or alternatively (and probably worst
			// of all) don't store in memory, but use an expensive iterative search
			// for non-distinct rows.
			//
			// An iterative search will be terrible for large groups with mostly
			// distinct rows.  But would be okay for large groups with few distinct
			// rows.

			if (group == null)
				throw new Exception("'count' can only be used as an aggregate function.");

			int rows = group.Count;
			if (rows <= 1) {
				// If count of entries in group is 0 or 1
				return TObject.GetInt4(rows);
			}

			// Make an array of all cells in the group that we are finding which
			// are distinct.
			int cols = ParameterCount;
			TObject[] group_r = new TObject[rows * cols];
			int n = 0;
			for (int i = 0; i < rows; ++i) {
				IVariableResolver vr = group.GetVariableResolver(i);
				for (int p = 0; p < cols; ++p) {
					Expression exp = this[p];
					group_r[n + p] = exp.Evaluate(null, vr, context);
				}
				n += cols;
			}

			// A comparator that sorts this set,
			IComparer c = new ComparerImpl(cols, group_r);

			// The list of indexes,
			Object[] list = new Object[rows];
			for (int i = 0; i < rows; ++i) {
				list[i] = i;
			}

			// Sort the list,
			Array.Sort(list, c);

			// The count of distinct elements, (there will always be at least 1)
			int distinct_count = 1;
			for (int i = 1; i < rows; ++i) {
				int v = c.Compare(list[i], list[i - 1]);
				// If v == 0 then entry is not distinct with the previous element in
				// the sorted list therefore the distinct counter is not incremented.
				if (v > 0) {
					// If current entry is greater than previous then we've found a
					// distinct entry.
					++distinct_count;
				} else if (v < 0) {
					// The current element should never be less if list is sorted in
					// ascending order.
					throw new ApplicationException("Assertion failed - the distinct list does not " +
					                               "appear to be sorted.");
				}
			}

			// If the first entry in the list is NULL then subtract 1 from the
			// distinct count because we shouldn't be counting NULL entries.
			if (list.Length > 0) {
				int first_entry = (int)list[0];
				// Assume first is null
				bool first_is_null = true;
				for (int m = 0; m < cols && first_is_null == true; ++m) {
					TObject val = group_r[(first_entry * cols) + m];
					if (!val.IsNull) {
						// First isn't null
						first_is_null = false;
					}
				}
				// Is first NULL?
				if (first_is_null) {
					// decrease distinct count so we don't count the null entry.
					distinct_count = distinct_count - 1;
				}
			}

			return TObject.GetInt4(distinct_count);
		}

		private class ComparerImpl : IComparer {
			private readonly int cols;
			private readonly TObject[] group_r;

			public ComparerImpl(int cols, TObject[] groupR) {
				this.cols = cols;
				group_r = groupR;
			}

			public int Compare(Object ob1, Object ob2) {
				int r1 = (int)ob1;
				int r2 = (int)ob2;

				// Compare row r1 with r2
				int index1 = r1 * cols;
				int index2 = r2 * cols;
				for (int n = 0; n < cols; ++n) {
					int v = group_r[index1 + n].CompareTo(group_r[index2 + n]);
					if (v != 0) {
						return v;
					}
				}

				// If we got here then rows must be equal.
				return 0;
			}
		}

	}
}