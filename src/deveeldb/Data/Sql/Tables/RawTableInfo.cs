// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Sql.Tables
{
	public sealed class RawTableInfo {
		private readonly List<RawTableItem> tableItems;

		private RawTableInfo(IEnumerable<RawTableItem> items) {
			tableItems = new List<RawTableItem>();

			if (items != null)
				tableItems.AddRange(items);
		}

		public RawTableInfo()
			: this(null) {
		}

		public void Add(IRootTable table, BigList<long> rowSet) {
			tableItems.Add(new RawTableItem(table, rowSet));
		}

		public IRootTable[] Tables => tableItems.Select(x => x.Table).ToArray();

		public BigList<long>[] Rows => tableItems.Select(x => x.Rows).ToArray();

		private RawTableItem[] SortItems() {
			var list = new RawTableItem[tableItems.Count];
			tableItems.CopyTo(list);
			Array.Sort(list);
			return list;
		}

		public RawTableInfo Union(RawTableInfo info) {
			// Number of Table 'columns'

			int colCount = tableItems.Count;

			var merge1 = SortItems();
			var merge2 = info.SortItems();

			long size1 = -1;
			long size2 = -1;

			// First check number of tables in each merge is correct.

			if (merge1.Length != merge2.Length)
				throw new InvalidOperationException("Incorrect format in table union");

			// Check each table in the merge1 set has identical length row_sets

			for (long i = 0; i < merge1.Length; ++i) {
				if (size1 == -1) {
					size1 = merge1[i].Rows.Count;
				} else {
					if (size1 != merge1[i].Rows.Count)
						throw new InvalidOperationException("Incorrect format in table union");
				}
			}

			// Check each table in the merge2 set has identical length row_sets

			for (long i = 0; i < merge2.Length; ++i) {
				// Check the tables in merge2 are identical to the tables in merge1
				if (!merge2[i].Table.Equals(merge1[i].Table))
					throw new InvalidOperationException("Incorrect format in table union");

				if (size2 == -1) {
					size2 = merge2[i].Rows.Count;
				} else {
					if (size2 != merge2[i].Rows.Count)
						throw new InvalidOperationException("Incorrect format in table union");
				}
			}

			// If size1 or size2 are -1 then we have a corrupt table.  (It will be
			// 0 for an empty table).

			if (size1 == -1 || size2 == -1)
				throw new InvalidOperationException("Incorrect format in table union");

			// We don't need information in 'raw_info' vector anymore so clear it.
			// This may help garbage collection.

			var resultItems = new List<RawTableItem>();

			// Merge the two together into a new list of RawRowElement[]

			var mergeSize = size1 + size2;
			var elems = new BigArray<RawRowItem>(mergeSize);
			int elemsIndex = 0;

			for (long i = 0; i < size1; ++i) {
				var itemRows = new BigArray<long>(colCount);

				for (int n = 0; n < colCount; ++n) {
					itemRows[n] = merge1[n].Rows[i];
				}

				elems[elemsIndex] = new RawRowItem(itemRows);
				++elemsIndex;
			}

			for (long i = 0; i < size2; ++i) {
				var itemRows = new BigArray<long>(colCount);

				for (int n = 0; n < colCount; ++n) {
					itemRows[n] = merge2[n].Rows[i];
				}

				elems[elemsIndex] = new RawRowItem(itemRows);
				++elemsIndex;
			}

			// Now sort the row elements into order.

			BigArray<RawRowItem>.QuickSort(elems);

			// Remove any duplicate rows.

			for (int i = 0; i < colCount; ++i) {
				merge1[i].Rows.Clear();
			}

			RawRowItem previous = null;
			for (int n = 0; n < mergeSize; ++n) {
				var current = elems[n];

				// Check that the current element in the set is not a duplicate of the
				// previous.

				if (previous == null || previous.CompareTo(current) != 0) {
					for (int i = 0; i < colCount; ++i) {
						merge1[i].Rows.Add(current.RowValues[i]);
					}
					previous = current;
				}
			}

			for (int i = 0; i < colCount; ++i) {
				resultItems.Add(merge1[i]);
			}

			return new RawTableInfo(resultItems.ToArray());
		}

		#region RawTableItem

		class RawTableItem : IComparable<RawTableItem> {
			public RawTableItem(IRootTable table)
				: this(table, new BigList<long>(0)) {
			}

			public RawTableItem(IRootTable table, BigList<long> rows) {
				Table = table;
				Rows = rows;
			}

			public IRootTable Table { get; }

			public BigList<long> Rows { get; }

			public int CompareTo(RawTableItem other) {
				return Table.GetHashCode() - other.Table.GetHashCode();
			}
		}

		#endregion

		#region RawRowItem

		class RawRowItem : IComparable<RawRowItem> {
			public RawRowItem(BigArray<long> values) {
				RowValues = values;
			}

			public BigArray<long> RowValues { get; private set; }

			public int CompareTo(RawRowItem other) {
				var size = RowValues.Length;
				for (var i = 0; i < size; ++i) {
					var v1 = RowValues[i];
					var v2 = other.RowValues[i];
					if (v1 != v2) {
						return (int) (v1 - v2);
					}
				}
				return 0;
			}
		}

		#endregion

	}
}
