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
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// This is a scheme that performs a blind search of a given set.
	/// </summary>
	/// <remarks>
	/// It records no information about how a set element relates to the 
	/// rest. It blindly searches through the set to find elements that 
	/// match the given criteria.
	/// <para>
	/// This scheme performs badly on large sets because it requires that 
	/// the database is queried often for information.  However since it 
	/// records no information about the set, memory requirements are 
	/// non-existant.
	/// </para>
	/// <para>
	/// This scheme should not be used for anything other than small domain 
	/// sets because the performance suffers very badly with larger sets.  
	/// It is ideal for small domain sets because of its no memory overhead.  
	/// For any select operation this algorithm must check every element in 
	/// the set.
	/// </para>
	/// </remarks>
	public sealed class BlindSearch : SelectableScheme {
		public BlindSearch(ITableDataSource table, int column)
			: base(table, column) {
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This scheme doesn't take any notice of insertions or removals.
		/// </remarks>
		public override void Insert(int row) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an immutable scheme.");
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This scheme doesn't take any notice of insertions or removals.
		/// </remarks>
		public override void Remove(int row) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an immutable scheme.");
		}

		/// <inheritdoc/>
		public override SelectableScheme Copy(ITableDataSource table, bool immutable) {
			// Return a fresh object.  This implementation has no state so we can
			// ignore the 'immutable' flag.
			return new BlindSearch(table, Column);
		}

		/// <inheritdoc/>
		public override void Dispose() {
			// Nothing to do!
		}

		/// <summary>
		/// Searches for a given <see cref="TObject"/> (<paramref name="ob"/>) in the 
		/// row list between the two bounds.
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="list"></param>
		/// <param name="lower"></param>
		/// <param name="higher"></param>
		/// <remarks>
		/// This returns the place to insert ob into the vector, it should not be used to 
		/// determine if <paramref name="ob"/> is in the list or not.
		/// </remarks>
		/// <returns>
		/// This will return the highest row of the set of values that are equal to <paramref name="ob"/>.
		/// </returns>
		private int HighestSearch(TObject ob, IList<int> list, int lower, int higher) {
			if ((higher - lower) <= 5) {
				// Start from the bottom up until we find the highest val
				for (int i = higher; i >= lower; --i) {
					int res = ob.CompareTo(GetCellContents(list[i]));
					if (res >= 0)
						return i + 1;
				}
				// Didn't find return lowest
				return lower;
			}

			int mid = (lower + higher) / 2;
			int compResult = ob.CompareTo(GetCellContents(list[mid]));

			if (compResult == 0)
				// We know the bottom is between 'mid' and 'higher'
				return HighestSearch(ob, list, mid, higher);

			if (compResult < 0)
				return HighestSearch(ob, list, lower, mid - 1);

			return HighestSearch(ob, list, mid + 1, higher);
		}


		private void DoInsertSort(IList<int> list, int row) {
			int listSize = list.Count;
			if (listSize == 0) {
				list.Add(row);
			} else {
				int point = HighestSearch(GetCellContents(row), list, 0, listSize - 1);
				if (point == listSize) {
					list.Add(row);
				} else {
					list.Insert(point, row);
				}
			}
		}

		/// <inheritdoc/>
		public override IList<int> SelectAll() {
			List<int> rowList = new List<int>(Table.RowCount);
			IRowEnumerator e = Table.GetRowEnumerator();
			while (e.MoveNext()) {
				DoInsertSort(rowList, e.RowIndex);
			}
			return rowList;
		}

		/// <inheritdoc/>
		public override IList<int> SelectRange(SelectableRange range) {
			int setSize = Table.RowCount;
			// If no items in the set return an empty set
			if (setSize == 0)
				return new List<int>(0);

			return SelectRange(new SelectableRange[] { range });
		}

		/// <inheritdoc/>
		public override IList<int> SelectRange(SelectableRange[] ranges) {
			int setSize = Table.RowCount;
			// If no items in the set return an empty set
			if (setSize == 0)
				return new List<int>(0);

			RangeChecker checker = new RangeChecker(this, ranges);
			return checker.Resolve();
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// Object used to during range check loop.
		/// </summary>
		sealed class RangeChecker {
			private readonly BlindSearch scheme;

			/// <summary>
			/// The sorted list of all items in the set created as a cache for finding
			/// the first and last values.
			/// </summary>
			private IList<int> sortedSet;

			// The list of flags for each check in the range.
			// Either 0 for no check, 1 for < or >, 2 for <= or >=.
			private readonly byte[] lowerFlags;
			private readonly byte[] upperFlags;

			// The TObject objects to check against.
			private readonly TObject[] lowerCells;
			private readonly TObject[] upperCells;

			private const byte NoCheck = 0;
			private const byte CheckLesserOrGreater = 1;
			private const byte CheckLesserEqualOrGreaterEqual = 2;

			/// <summary>
			/// Constructs the checker.
			/// </summary>
			/// <param name="scheme"></param>
			/// <param name="ranges"></param>
			public RangeChecker(BlindSearch scheme, SelectableRange[] ranges) {
				int size = ranges.Length;
				lowerFlags = new byte[size];
				upperFlags = new byte[size];
				lowerCells = new TObject[size];
				upperCells = new TObject[size];
				for (int i = 0; i < ranges.Length; ++i) {
					SetupRange(i, ranges[i]);
				}
				this.scheme = scheme;
			}

			private void ResolveSortedSet() {
				if (sortedSet == null) {
					sortedSet = scheme.SelectAll();
				}
			}

			/// <summary>
			/// Resolves a cell.
			/// </summary>
			/// <param name="ob"></param>
			/// <returns></returns>
			private TObject ResolveCell(TObject ob) {
				if (ob == SelectableRange.FirstInSet) {
					ResolveSortedSet();
					return scheme.GetCellContents(sortedSet[0]);

				}
				if (ob == SelectableRange.LastInSet) {
					ResolveSortedSet();
					return scheme.GetCellContents(sortedSet[sortedSet.Count - 1]);
				}

				return ob;
			}

			/// <summary>
			/// Set up a range.
			/// </summary>
			/// <param name="i"></param>
			/// <param name="range"></param>
			private void SetupRange(int i, SelectableRange range) {
				TObject l = range.Start;
				RangePosition lf = range.StartPosition;
				TObject u = range.End;
				RangePosition uf = range.EndPosition;

				// Handle lower first
				if (l == SelectableRange.FirstInSet &&
					lf == RangePosition.FirstValue) {
					// Special case no lower check
					lowerFlags[i] = NoCheck;
				} else {
					if (lf == RangePosition.FirstValue) {
						lowerFlags[i] = CheckLesserEqualOrGreaterEqual;  // >=
					} else if (lf == RangePosition.AfterLastValue) {
						lowerFlags[i] = CheckLesserOrGreater;  // >
					} else {
						throw new ApplicationException("Incorrect lower flag.");
					}
					lowerCells[i] = ResolveCell(l);
				}

				// Now handle upper
				if (u == SelectableRange.LastInSet &&
					uf == RangePosition.LastValue) {
					// Special case no upper check
					upperFlags[i] = NoCheck;
				} else {
					if (uf == RangePosition.LastValue) {
						upperFlags[i] = CheckLesserEqualOrGreaterEqual;  // <=
					} else if (uf == RangePosition.BeforeFirstValue) {
						upperFlags[i] = CheckLesserOrGreater;  // <
					} else {
						throw new ApplicationException("Incorrect upper flag.");
					}
					upperCells[i] = ResolveCell(u);
				}

			}

			/// <summary>
			/// Resolves the ranges.
			/// </summary>
			/// <returns></returns>
			public IList<int> Resolve() {
				// The idea here is to only need to scan the column once to find all
				// the cells that meet our criteria.
				List<int> list = new List<int>();
				IRowEnumerator e = scheme.Table.GetRowEnumerator();

				int compare_tally = 0;

				int size = lowerFlags.Length;
				while (e.MoveNext()) {
					int row = e.RowIndex;
					// For each range
					for (int i = 0; i < size; ++i) {
						bool result = true;
						byte lf = lowerFlags[i];
						if (lf != NoCheck) {
							++compare_tally;
							TObject v = scheme.GetCellContents(row);
							int compare = lowerCells[i].CompareTo(v);
							if (lf == CheckLesserOrGreater) {  // >
								result = (compare < 0);
							} else if (lf == CheckLesserEqualOrGreaterEqual) {  // >=
								result = (compare <= 0);
							} else {
								throw new ApplicationException("Incorrect flag.");
							}
						}
						if (result) {
							byte uf = upperFlags[i];
							if (uf != NoCheck) {
								++compare_tally;
								TObject v = scheme.GetCellContents(row);
								int compare = upperCells[i].CompareTo(v);
								if (uf == CheckLesserOrGreater) {  // <
									result = (compare > 0);
								} else if (uf == CheckLesserEqualOrGreaterEqual) {  // >=
									result = (compare >= 0);
								} else {
									throw new ApplicationException("Incorrect flag.");
								}
							}
							// Pick this row
							if (result) {
								scheme.DoInsertSort(list, row);
								break;
							}
						}
					}
				}

				return list;
			}

		}
	}
}