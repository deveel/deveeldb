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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql {
	public sealed class RawTableInfo {
		private readonly List<RawTableItem> tableItems;

		public RawTableInfo() {
			tableItems = new List<RawTableItem>();
		}

		private RawTableInfo(IEnumerable<RawTableItem> tableItems)
			: this() {
			this.tableItems.AddRange(tableItems);
		}

		public void Add(IRootTable table, IList<int> row_set) {
			tableItems.Add(new RawTableItem(table, row_set));
		}

		public ITable[] GetTables() {
			return tableItems.Select(x => x.Table).Cast<ITable>().ToArray();
		}

		public IList<int>[] GetRows() {
			return tableItems.Select(x => x.Rows).ToArray();
		}

		private RawTableItem[] GetSortedItems() {
			var list = new RawTableItem[tableItems.Count];
			tableItems.CopyTo(list);
			Array.Sort(list);
			return list;
		}

		public RawTableInfo Union(RawTableInfo info) {
			// Number of Table 'columns'

			int colCount = tableItems.Count;

			var merge1 = GetSortedItems();
			var merge2 = info.GetSortedItems();

			int size1 = -1;
			int size2 = -1;

			// First check number of tables in each merge is correct.

			if (merge1.Length != merge2.Length)
				throw new ApplicationException("Incorrect format in table union");

			// Check each table in the merge1 set has identical length row_sets

			for (int i = 0; i < merge1.Length; ++i) {
				if (size1 == -1) {
					size1 = merge1[i].Rows.Count;
				} else {
					if (size1 != merge1[i].Rows.Count)
						throw new ApplicationException("Incorrect format in table union");
				}
			}

			// Check each table in the merge2 set has identical length row_sets

			for (int i = 0; i < merge2.Length; ++i) {
				// Check the tables in merge2 are identical to the tables in merge1
				if (!merge2[i].Table.Equals(merge1[i].Table))
					throw new ApplicationException("Incorrect format in table union");

				if (size2 == -1) {
					size2 = merge2[i].Rows.Count;
				} else {
					if (size2 != merge2[i].Rows.Count)
						throw new ApplicationException("Incorrect format in table union");
				}
			}

			// If size1 or size2 are -1 then we have a corrupt table.  (It will be
			// 0 for an empty table).

			if (size1 == -1 || size2 == -1)
				throw new ApplicationException("Incorrect format in table union");

			// We don't need information in 'raw_info' vector anymore so clear it.
			// This may help garbage collection.

			var resultItems = new List<RawTableItem>();

			// Merge the two together into a new list of RawRowElement[]

			int mergeSize = size1 + size2;
			var elems = new RawRowItem[mergeSize];
			int elemsIndex = 0;

			for (int i = 0; i < size1; ++i) {
				var itemRows = new int[colCount];

				for (int n = 0; n < colCount; ++n) {
					itemRows[n] = merge1[n].Rows[i];
				}

				elems[elemsIndex] = new RawRowItem(itemRows);
				++elemsIndex;
			}

			for (int i = 0; i < size2; ++i) {
				var itemRows = new int[colCount];

				for (int n = 0; n < colCount; ++n) {
					itemRows[n] = merge2[n].Rows[i];
				}

				elems[elemsIndex] = new RawRowItem(itemRows);
				++elemsIndex;
			}

			// Now sort the row elements into order.

			Array.Sort(elems);

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

			return new RawTableInfo(resultItems.AsReadOnly());
		}

		public RawTableInfo RemoveDuplicates() {
			// If no tables in duplicate then return

			if (tableItems.Count == 0)
				return new RawTableInfo();

			// Get the length of the first row set in the first table.  We assume that
			// the row set length is identical across each table in the Vector.

			var elen = tableItems[0];
			int len = elen.Rows.Count;
			if (len == 0)
				return new RawTableInfo(tableItems.AsReadOnly());

			// Create a new row element to sort.

			var elems = new RawRowItem[len];
			int width = tableItems.Count;

			// Create an array of RawTableElement so we can quickly access the data

			var rdup = new RawTableItem[width];
			tableItems.CopyTo(rdup);

			// Run through the data building up a new RawTableElement[] array with
			// the information in every raw span.

			for (int i = 0; i < len; ++i) {
				var itemRows = new int[width];
				for (int n = 0; n < width; ++n) {
					itemRows[n] = rdup[n].Rows[i];
				}
				elems[i] = new RawRowItem(itemRows);
			}

			// Now 'elems' it an array of individual RawRowItem objects which
			// represent each individual row in the table.

			// Now sort and remove duplicates to make up a new set.

			Array.Sort(elems);

			var resultTables = new List<RawTableItem>();

			// Make a new set of RawTableElement[] objects

			var items = rdup;

			// Set up the 'raw_info' vector with the new RawTableElement[] removing
			// any duplicate rows.

			for (int i = 0; i < width; ++i) {
				items[i].Rows.Clear();
			}

			RawRowItem previous = null;
			for (int n = 0; n < len; ++n) {
				var current = elems[n];

				// Check that the current element in the set is not a duplicate of the
				// previous.

				if (previous == null || previous.CompareTo(current) != 0) {
					for (int i = 0; i < width; ++i) {
						items[i].Rows.Add(current.RowValues[i]);
					}
					previous = current;
				}
			}

			for (int i = 0; i < width; ++i) {
				resultTables.Add(items[i]);
			}

			return new RawTableInfo(resultTables.AsReadOnly());
		}


		#region RawTableItem

		class RawTableItem : IComparable<RawTableItem> {
			public RawTableItem(IRootTable table)
				: this(table, new List<int>()) {
			}

			public RawTableItem(IRootTable table, IList<int> rows) {
				Table = table;
				Rows = new List<int>(rows);
			}

			public IRootTable Table { get; private set; }

			public IList<int> Rows { get; private set; } 

			public int CompareTo(RawTableItem other) {
				return Table.GetHashCode() - other.Table.GetHashCode();
			}
		}

		#endregion

		#region RawRowItem

		class RawRowItem : IComparable<RawRowItem> {
			public RawRowItem(int[] values) {
				RowValues = values;
			}

			public int[] RowValues { get; private set; }

			public int CompareTo(RawRowItem other) {
				int size = RowValues.Length;
				for (int i = 0; i < size; ++i) {
					int v1 = RowValues[i];
					int v2 = other.RowValues[i];
					if (v1 != v2) {
						return v1 - v2;
					}
				}
				return 0;
			}
		}

		#endregion
	}
}
