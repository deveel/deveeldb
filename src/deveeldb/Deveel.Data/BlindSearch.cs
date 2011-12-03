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
using System.IO;

using Deveel.Data.Collections;
using Deveel.Data.Util;

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
		internal override void Insert(int row) {
			if (IsImmutable) {
				throw new ApplicationException("Tried to change an immutable scheme.");
			}
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This scheme doesn't take any notice of insertions or removals.
		/// </remarks>
		internal override void Remove(int row) {
			if (IsImmutable) {
				throw new ApplicationException("Tried to change an immutable scheme.");
			}
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

		/**
		 * We implement an insert sort algorithm here.  Each new row is inserted
		 * into our row vector at the sorted corrent position.
		 * The algorithm assumes the given vector is already sorted.  We then just
		 * subdivide the set until we can insert at the required position.
		 */
		private int Search(TObject ob, IntegerVector vec, int lower, int higher) {
			if (lower >= higher) {
				if (ob.CompareTo(GetCellContents(vec[lower])) > 0) {
					return lower + 1;
				} else {
					return lower;
				}
			}

			int mid = lower + ((higher - lower) / 2);
			int comp_result = ob.CompareTo(GetCellContents(vec[mid]));

			if (comp_result == 0) {
				return mid;
			} else if (comp_result < 0) {
				return Search(ob, vec, lower, mid - 1);
			} else {
				return Search(ob, vec, mid + 1, higher);
			}

		}

		/// <summary>
		/// Searches for a given <see cref="TObject"/> (<paramref name="ob"/>) in the 
		/// row list between the two bounds.
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="vec"></param>
		/// <param name="lower"></param>
		/// <param name="higher"></param>
		/// <remarks>
		/// This returns the place to insert ob into the vector, it should not be used to 
		/// determine if <paramref name="ob"/> is in the list or not.
		/// </remarks>
		/// <returns>
		/// This will return the highest row of the set of values that are equal to <paramref name="ob"/>.
		/// </returns>
		private int HighestSearch(TObject ob, IntegerVector vec, int lower, int higher) {

			if ((higher - lower) <= 5) {
				// Start from the bottom up until we find the highest val
				for (int i = higher; i >= lower; --i) {
					int res = ob.CompareTo(GetCellContents(vec[i]));
					if (res >= 0) {
						return i + 1;
					}
				}
				// Didn't find return lowest
				return lower;
			}

			int mid = (lower + higher) / 2;
			int comp_result = ob.CompareTo(GetCellContents(vec[mid]));

			if (comp_result == 0) {
				// We know the bottom is between 'mid' and 'higher'
				return HighestSearch(ob, vec, mid, higher);
			} else if (comp_result < 0) {
				return HighestSearch(ob, vec, lower, mid - 1);
			} else {
				return HighestSearch(ob, vec, mid + 1, higher);
			}
		}


		private void DoInsertSort(IntegerVector vec, int row) {
			int list_size = vec.Count;
			if (list_size == 0) {
				vec.AddInt(row);
			} else {
				int point = HighestSearch(GetCellContents(row), vec, 0, list_size - 1);
				if (point == list_size) {
					vec.AddInt(row);
				} else {
					vec.InsertIntAt(row, point);
				}
			}
		}

		/// <inheritdoc/>
		public override IntegerVector SelectAll() {
			IntegerVector row_list = new IntegerVector(Table.RowCount);
			IRowEnumerator e = Table.GetRowEnumerator();
			while (e.MoveNext()) {
				DoInsertSort(row_list, e.RowIndex);
			}
			return row_list;
		}

		/// <inheritdoc/>
		internal override IntegerVector SelectRange(SelectableRange range) {
			int set_size = Table.RowCount;
			// If no items in the set return an empty set
			if (set_size == 0) {
				return new IntegerVector(0);
			}

			return SelectRange(new SelectableRange[] { range });
		}

		/// <inheritdoc/>
		internal override IntegerVector SelectRange(SelectableRange[] ranges) {
			int set_size = Table.RowCount;
			// If no items in the set return an empty set
			if (set_size == 0) {
				return new IntegerVector(0);
			}

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
			private IntegerVector sorted_set = null;

			// The list of flags for each check in the range.
			// Either 0 for no check, 1 for < or >, 2 for <= or >=.
			private byte[] lower_flags;
			private byte[] upper_flags;

			// The TObject objects to check against.
			private TObject[] lower_cells;
			private TObject[] upper_cells;

			/// <summary>
			/// Constructs the checker.
			/// </summary>
			/// <param name="scheme"></param>
			/// <param name="ranges"></param>
			public RangeChecker(BlindSearch scheme, SelectableRange[] ranges) {
				int size = ranges.Length;
				lower_flags = new byte[size];
				upper_flags = new byte[size];
				lower_cells = new TObject[size];
				upper_cells = new TObject[size];
				for (int i = 0; i < ranges.Length; ++i) {
					SetupRange(i, ranges[i]);
				}
				this.scheme = scheme;
			}

			private void ResolveSortedSet() {
				if (sorted_set == null) {
		//        Console.Out.WriteLine("SLOW RESOLVE SORTED SET ON BLIND SEARCH.");
					sorted_set = scheme.SelectAll();
				}
			}

			/// <summary>
			/// Resolves a cell.
			/// </summary>
			/// <param name="ob"></param>
			/// <returns></returns>
			private TObject ResolveCell(TObject ob) {
				if (ob == SelectableRange.FIRST_IN_SET) {
					ResolveSortedSet();
					return scheme.GetCellContents(sorted_set[0]);

				} else if (ob == SelectableRange.LAST_IN_SET) {
					ResolveSortedSet();
					return scheme.GetCellContents(sorted_set[sorted_set.Count - 1]);
				} else {
					return ob;
				}
			}

			/// <summary>
			/// Set up a range.
			/// </summary>
			/// <param name="i"></param>
			/// <param name="range"></param>
			private void SetupRange(int i, SelectableRange range) {
				TObject l = range.Start;
				byte lf = range.StartFlag;
				TObject u = range.End;
				byte uf = range.EndFlag;

				// Handle lower first
				if (l == SelectableRange.FIRST_IN_SET &&
					lf == SelectableRange.FIRST_VALUE) {
					// Special case no lower check
					lower_flags[i] = 0;
				} else {
					if (lf == SelectableRange.FIRST_VALUE) {
						lower_flags[i] = 2;  // >=
					} else if (lf == SelectableRange.AFTER_LAST_VALUE) {
						lower_flags[i] = 1;  // >
					} else {
						throw new ApplicationException("Incorrect lower flag.");
					}
					lower_cells[i] = ResolveCell(l);
				}

				// Now handle upper
				if (u == SelectableRange.LAST_IN_SET &&
					uf == SelectableRange.LAST_VALUE) {
					// Special case no upper check
					upper_flags[i] = 0;
				} else {
					if (uf == SelectableRange.LAST_VALUE) {
						upper_flags[i] = 2;  // <=
					} else if (uf == SelectableRange.BEFORE_FIRST_VALUE) {
						upper_flags[i] = 1;  // <
					} else {
						throw new ApplicationException("Incorrect upper flag.");
					}
					upper_cells[i] = ResolveCell(u);
				}

			}

			/// <summary>
			/// Resolves the ranges.
			/// </summary>
			/// <returns></returns>
			public IntegerVector Resolve() {
				// The idea here is to only need to scan the column once to find all
				// the cells that meet our criteria.
				IntegerVector ivec = new IntegerVector();
				IRowEnumerator e = scheme.Table.GetRowEnumerator();

				int compare_tally = 0;

				int size = lower_flags.Length;
				while (e.MoveNext()) {
					int row = e.RowIndex;
					// For each range
					//TODO: check ... 
					//range_set:
					for (int i = 0; i < size; ++i) {
						bool result = true;
						byte lf = lower_flags[i];
						if (lf != 0) {
							++compare_tally;
							TObject v = scheme.GetCellContents(row);
							int compare = lower_cells[i].CompareTo(v);
							if (lf == 1) {  // >
								result = (compare < 0);
							} else if (lf == 2) {  // >=
								result = (compare <= 0);
							} else {
								throw new ApplicationException("Incorrect flag.");
							}
						}
						if (result) {
							byte uf = upper_flags[i];
							if (uf != 0) {
								++compare_tally;
								TObject v = scheme.GetCellContents(row);
								int compare = upper_cells[i].CompareTo(v);
								if (uf == 1) {  // <
									result = (compare > 0);
								} else if (uf == 2) {  // >=
									result = (compare >= 0);
								} else {
									throw new ApplicationException("Incorrect flag.");
								}
							}
							// Pick this row
							if (result) {
								scheme.DoInsertSort(ivec, row);
								//TODO: check this
								// break range_set;
								break;
							}
						}
					}
				}

				//      Console.Out.WriteLine("Blind Search compare tally: " + compare_tally);

				return ivec;
			}

		}
	}
}