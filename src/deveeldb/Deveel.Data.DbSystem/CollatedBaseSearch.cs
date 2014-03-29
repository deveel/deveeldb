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

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An implementation of <see cref="SelectableScheme"/> that is 
	/// based on some collated set of data.
	/// </summary>
	/// <remarks>
	/// This can be used to implement more advanced types of selectable 
	/// schemes based on presistant indexes (see <see cref="InsertSearch"/>).
	/// <para>
	/// The default implementation maintains no state,
	/// </para>
	/// <para>
	/// Derived classes are required to implement <see cref="SelectableScheme.Copy"/>, 
	/// <see cref="SearchFirst"/> and <see cref="SearchLast"/> methods.  
	/// With these basic methods, a selectable scheme can be generated 
	/// provided the column is sorted in ascending order (value of row 
	/// i is &lt;= value of row i+1).  Overwrite <see cref="FirstInCollationOrder"/>,
	/// <see cref="LastInCollationOrder"/> and <see cref="AddRangeToSet"/>
	/// methods for non sorted underlying sets.
	/// </para>
	/// </remarks>
	public abstract class CollatedBaseSearch : SelectableScheme {
		protected CollatedBaseSearch(ITableDataSource table, int column)
			: base(table, column) {
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This oveeride method does nothing.
		/// </remarks>
		public override void Insert(int row) {
			// Ignore insert (no state to maintain)
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an immutable scheme.");
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This oveeride method does nothing.
		/// </remarks>
		public override void Remove(int row) {
			// Ignore remove (no state to maintain)
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an immutable scheme.");
		}

		/// <inheritdoc/>
		public override void Dispose() {
			// Nothing to do!
		}


		// -------- Abstract or overwrittable methods ----------

		/// <summary>
		/// Finds the position in the collated set of the first value in the 
		/// column equal to the given value.
		/// </summary>
		/// <param name="val">The value to search in the scheme.</param>
		/// <returns>
		/// Returns the position (as <see cref="Int32">integer</see>) of the 
		/// first value in the column equal to the given <paramref name="val"/> 
		/// or <c>-(insert_position + 1)</c> if the value is not to be found in the 
		/// column.
		/// </returns>
		protected abstract int SearchFirst(TObject val);

		/// <summary>
		/// Finds the position in the collated set of the last value in the 
		/// column equal to the given value. 
		/// </summary>
		/// <param name="val">The value to search in the scheme.</param>
		/// <returns>
		/// Returns the position (as <see cref="Int32">integer</see>) of the 
		/// last value in the column equal to the given <paramref name="val"/> 
		/// or <c>-(insert_position + 1)</c> if the value is not to be found 
		/// in the column.
		/// </returns>
		protected abstract int SearchLast(TObject val);

		/// <summary>
		/// Gets the size of the set (the number of rows in this column).
		/// </summary>
		protected virtual int SetSize {
			get { return Table.RowCount; }
		}

		/// <summary>
		/// Returns the first value of this column (in collated order).
		/// </summary>
		/// <remarks>
		/// For example, if the column contains (1, 4, 8} then '1' is returned.
		/// </remarks>
		protected virtual TObject FirstInCollationOrder {
			get { return GetCellContents(0); }
		}

		/// <summary>
		/// Returns the last value of this column (in collated order).
		/// </summary>
		/// <remarks>
		/// For example, if the column contains (1, 4, 8} then '8' is returned.
		/// </remarks>
		protected virtual TObject LastInCollationOrder {
			get { return GetCellContents(SetSize - 1); }
		}

		/// <summary>
		/// Adds the set indexes to the list that represent the range of values
		/// between the start (inclusive) and end offset (inclusive) given.
		/// </summary>
		/// <param name="start">The offset where to start adding the given range.</param>
		/// <param name="end">The offset where to end adding the given range.</param>
		/// <param name="list"></param>
		/// <returns>
		/// Returns the given <paramref name="list"/> integrated with the given
		/// range of integers.
		/// </returns>
		protected virtual IList<int> AddRangeToSet(int start, int end, IList<int> list) {
			if (list == null)
				list = new List<int>((end - start) + 2);

			for (int i = start; i <= end; ++i) {
				list.Add(i);
			}

			return list;
		}

		// ---------- Range search methods ----------

		public override IList<int> SelectAll() {
			return AddRangeToSet(0, SetSize - 1, null);
		}

		/// <summary>
		/// Given a point and a value which is either a place marker (first, 
		/// last in set) or a <see cref="TObject"/> object, this will determine 
		/// the position in this set of the range point.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="val"></param>
		/// <remarks>
		/// For example, we may want to know the index of the last instance of 
		/// a particular number in a set of numbers which would be 
		/// <c>positionOfRangePoint(SelectableRangePoint.LastValue, [number TObject])</c>.
		/// <para>
		/// Note how the position is determined if the value is not found in the set.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private int PositionOfRangePoint(RangePosition position, TObject val) {
			int p;
			TObject cell;

			switch (position) {

				case RangePosition.FirstValue:
					if (val == SelectableRange.FirstInSet) {
						return 0;
					}
					if (val == SelectableRange.LastInSet) {
						// Get the last value and search for the first instance of it.
						cell = LastInCollationOrder;
					} else {
						cell = val;
					}
					p = SearchFirst(cell);
					// (If value not found)
					if (p < 0) {
						return -(p + 1);
					}
					return p;

				case RangePosition.LastValue:
					if (val == SelectableRange.LastInSet) {
						return SetSize - 1;
					}
					if (val == SelectableRange.FirstInSet) {
						// Get the first value.
						cell = FirstInCollationOrder;
					} else {
						cell = val;
					}
					p = SearchLast(cell);
					// (If value not found)
					if (p < 0) {
						return -(p + 1) - 1;
					}
					return p;

				case RangePosition.BeforeFirstValue:
					if (val == SelectableRange.FirstInSet)
						return -1;

					if (val == SelectableRange.LastInSet) {
						// Get the last value and search for the first instance of it.
						cell = LastInCollationOrder;
					} else {
						cell = val;
					}

					p = SearchFirst(cell);
					// (If value not found)
					if (p < 0) {
						return -(p + 1) - 1;
					}
					return p - 1;

				case RangePosition.AfterLastValue:
					if (val == SelectableRange.LastInSet) {
						return SetSize;
					}
					if (val == SelectableRange.FirstInSet) {
						// Get the first value.
						cell = FirstInCollationOrder;
					} else {
						cell = val;
					}
					p = SearchLast(cell);
					// (If value not found)
					if (p < 0) {
						return -(p + 1);
					}
					return p + 1;

				default:
					throw new ApplicationException("Unrecognised position.");
			}

		}

		/// <summary>
		/// Adds a range from this set to the given list of integers.
		/// </summary>
		/// <param name="range">The instance of <see cref="SelectableRange"/>
		/// used to identify the indexes of nodes to add to the given list.</param>
		/// <param name="list">The list of integers where to add the selected range
		/// of indexes.</param>
		/// <remarks>
		/// IntegerList may be null if a list has not yet been allocated for 
		/// the range.
		/// </remarks>
		/// <returns>
		/// Returns the given <paramref name="list"/> integrated with the range
		/// of values identified by the given <pramref name="range"/> selector.
		/// </returns>
		private IList<int> AddRange(SelectableRange range, IList<int> list) {
			// Select the range specified.
			RangePosition startFlag = range.StartPosition;
			TObject start = range.Start;
			RangePosition endFlag = range.EndPosition;
			TObject end = range.End;

			int r1 = PositionOfRangePoint(startFlag, start);
			int r2 = PositionOfRangePoint(endFlag, end);

			if (r2 < r1)
				return list;

			// Add the range to the set
			return AddRangeToSet(r1, r2, list);

		}

		/// <inheritdoc/>
		public override IList<int> SelectRange(SelectableRange range) {
			// If no items in the set return an empty set
			if (SetSize == 0)
				return new List<int>(0);

			IList<int> list = AddRange(range, null);
			if (list == null)
				return new List<int>(0);

			return list;
		}

		/// <inheritdoc/>
		public override IList<int> SelectRange(SelectableRange[] ranges) {
			// If no items in the set return an empty set
			if (SetSize == 0)
				return new List<int>(0);

			IList<int> list = null;
			foreach (SelectableRange range in ranges) {
				list = AddRange(range, list);
			}

			if (list == null)
				return new List<int>(0);

			return list;
		}
	}
}