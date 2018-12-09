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

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Indexes {
	public sealed class BlindSearchIndex : TableIndex {
		public BlindSearchIndex(IndexInfo indexInfo, ITable table) : base(indexInfo, table) {
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new ArgumentException("Cannot mutate a read-only index.");
		}

		private long HighestSearch(IndexKey ob, BigList<long> list, long lower, long higher) {
			if ((higher - lower) <= 5) {
				// Start from the bottom up until we find the highest val
				for (var i = higher; i >= lower; --i) {
					int res = ob.CompareTo(GetKey(list[i]));
					if (res >= 0)
						return i + 1;
				}
				// Didn't find return lowest
				return lower;
			}

			var mid = (lower + higher) / 2;
			int compResult = ob.CompareTo(GetKey(list[mid]));

			if (compResult == 0)
				// We know the bottom is between 'mid' and 'higher'
				return HighestSearch(ob, list, mid, higher);

			if (compResult < 0)
				return HighestSearch(ob, list, lower, mid - 1);

			return HighestSearch(ob, list, mid + 1, higher);
		}

		private void DoInsertSort(BigList<long> list, long row) {
			var listSize = list.Count;
			if (listSize == 0) {
				list.Add(row);
			} else {
				var point = HighestSearch(GetKey(row), list, 0, listSize - 1);
				if (point == listSize) {
					list.Add(row);
				} else {
					list.Insert(point, row);
				}
			}
		}

		public override IEnumerable<long> SelectRange(IndexRange[] ranges) {
			if (ranges.Length == 1 &&
			    ranges[0] == IndexRange.FullRange) {
				var rowList = new BigList<long>(Table.RowCount);
				using (var e = Table.GetEnumerator()) {
					while (e.MoveNext()) {
						DoInsertSort(rowList, e.Current.Number);
					}
				}

				return rowList;
			}

			var setSize = Table.RowCount;
			// If no items in the set return an empty set
			if (setSize == 0)
				return new long[0];

			var checker = new RangeChecker(this, ranges);
			return checker.Resolve();
		}

		public override void Insert(long row) {
			AssertNotReadOnly();
		}

		public override void Remove(long row) {
			AssertNotReadOnly();
		}

		#region RangeChecker

		class RangeChecker {
			private readonly BlindSearchIndex index;

			private BigArray<long> sortedSet;

			// The list of flags for each check in the range.
			// Either 0 for no check, 1 for < or >, 2 for <= or >=.
			private readonly byte[] lowerFlags;
			private readonly byte[] upperFlags;

			// The TObject objects to check against.
			private readonly IndexKey[] lowerCells;
			private readonly IndexKey[] upperCells;

			public RangeChecker(BlindSearchIndex index, IndexRange[] ranges) {
				this.index = index;

				var size = ranges.Length;
				lowerFlags = new byte[size];
				upperFlags = new byte[size];
				lowerCells = new IndexKey[size];
				upperCells = new IndexKey[size];

				for (int i = 0; i < ranges.Length; ++i) {
					SetupRange(i, ranges[i]);
				}
			}

			private const byte NoCheck = 0;
			private const byte CheckLesserOrGreater = 1;
			private const byte CheckLesserEqualOrGreaterEqual = 2;

			private void ResolveSortedSet() {
				if (sortedSet == null) {
					sortedSet = index.SelectAll().ToBigArray();
				}
			}

			private IndexKey ResolveKey(IndexKey key) {
				if (key.Equals(IndexRange.FirstInSet)) {
					ResolveSortedSet();
					return index.GetKey(sortedSet[0]);

				}
				if (key.Equals(IndexRange.LastInSet)) {
					ResolveSortedSet();
					return index.GetKey(sortedSet[sortedSet.Length - 1]);
				}

				return key;
			}

			private void SetupRange(int i, IndexRange range) {
				var l = range.StartValue;
				var lf = range.StartOffset;
				var u = range.EndValue;
				var uf = range.EndOffset;

				// Handle lower first
				if (l.Equals(IndexRange.FirstInSet) &&
				    lf.Equals(RangeFieldOffset.FirstValue)) {
					// Special case no lower check
					lowerFlags[i] = NoCheck;
				} else {
					if (lf.Equals(RangeFieldOffset.FirstValue)) {
						lowerFlags[i] = CheckLesserEqualOrGreaterEqual;  // >=
					} else if (lf.Equals(RangeFieldOffset.AfterLastValue)) {
						lowerFlags[i] = CheckLesserOrGreater;  // >
					} else {
						throw new InvalidOperationException("Incorrect lower flag.");
					}
					lowerCells[i] = ResolveKey(l);
				}

				// Now handle upper
				if (u.Equals(IndexRange.LastInSet) &&
				    uf.Equals(RangeFieldOffset.LastValue)) {
					// Special case no upper check
					upperFlags[i] = NoCheck;
				} else {
					if (uf.Equals(RangeFieldOffset.LastValue)) {
						upperFlags[i] = CheckLesserEqualOrGreaterEqual;  // <=
					} else if (uf.Equals(RangeFieldOffset.BeforeFirstValue)) {
						upperFlags[i] = CheckLesserOrGreater;  // <
					} else {
						throw new InvalidOperationException("Incorrect upper flag.");
					}
					upperCells[i] = ResolveKey(u);
				}
			}

			public IEnumerable<long> Resolve() {
				// The idea here is to only need to scan the column once to find all
				// the cells that meet our criteria.
				var list = new BigList<long>();
				using (var e = index.Table.GetEnumerator()) {

					int compareTally = 0;

					int size = lowerFlags.Length;
					while (e.MoveNext()) {
						var row = e.Current.Number;
						// For each range
						for (int i = 0; i < size; ++i) {
							bool result = true;
							byte lf = lowerFlags[i];
							if (lf != NoCheck) {
								++compareTally;
								var v = index.GetKey(row);
								int compare = lowerCells[i].CompareTo(v);
								if (lf == CheckLesserOrGreater) {
									// >
									result = (compare < 0);
								} else if (lf == CheckLesserEqualOrGreaterEqual) {
									// >=
									result = (compare <= 0);
								} else {
									throw new InvalidOperationException("Incorrect flag.");
								}
							}
							if (result) {
								byte uf = upperFlags[i];
								if (uf != NoCheck) {
									++compareTally;
									var v = index.GetKey(row);
									int compare = upperCells[i].CompareTo(v);
									if (uf == CheckLesserOrGreater) {
										// <
										result = (compare > 0);
									} else if (uf == CheckLesserEqualOrGreaterEqual) {
										// >=
										result = (compare >= 0);
									} else {
										throw new InvalidOperationException("Incorrect flag.");
									}
								}
								// Pick this row
								if (result) {
									index.DoInsertSort(list, row);
									break;
								}
							}
						}
					}

					return list;
				}
			}
		}

		#endregion
	}
}