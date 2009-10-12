//  
//  CollatedBaseSearch.cs
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
using System.IO;

using Deveel.Data.Collections;
using Deveel.Data.Util;

namespace Deveel.Data {
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
		internal override void Insert(int row) {
			// Ignore insert (no state to maintain)
			if (IsImmutable)
				throw new ApplicationException("Tried to change an immutable scheme.");
		}

		/// <inheritdoc/>
		/// <remarks>
		/// This oveeride method does nothing.
		/// </remarks>
		internal override void Remove(int row) {
			// Ignore remove (no state to maintain)
			if (IsImmutable) {
				throw new ApplicationException("Tried to change an immutable scheme.");
			}
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
		/// <param name="ivec"></param>
		/// <returns>
		/// Returns the given <paramref name="ivec"/> integrated with the given
		/// range of integers.
		/// </returns>
		protected virtual IntegerVector AddRangeToSet(int start, int end, IntegerVector ivec) {
			if (ivec == null) {
				ivec = new IntegerVector((end - start) + 2);
			}
			for (int i = start; i <= end; ++i) {
				ivec.AddInt(i);
			}
			return ivec;
		}

		// ---------- Range search methods ----------

		public override IntegerVector SelectAll() {
			return AddRangeToSet(0, SetSize - 1, null);
		}

		/// <summary>
		/// Given a point and a value which is either a place marker (first, 
		/// last in set) or a <see cref="TObject"/> object, this will determine 
		/// the position in this set of the range point.
		/// </summary>
		/// <param name="flag"></param>
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
		private int PositionOfRangePoint(byte flag, TObject val) {
			int p;
			TObject cell;

			switch (flag) {

				case (SelectableRange.FIRST_VALUE):
					if (val == SelectableRange.FIRST_IN_SET) {
						return 0;
					}
					if (val == SelectableRange.LAST_IN_SET) {
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

				case (SelectableRange.LAST_VALUE):
					if (val == SelectableRange.LAST_IN_SET) {
						return SetSize - 1;
					}
					if (val == SelectableRange.FIRST_IN_SET) {
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

				case (SelectableRange.BEFORE_FIRST_VALUE):
					if (val == SelectableRange.FIRST_IN_SET) {
						return -1;
					}
					if (val == SelectableRange.LAST_IN_SET) {
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

				case (SelectableRange.AFTER_LAST_VALUE):
					if (val == SelectableRange.LAST_IN_SET) {
						return SetSize;
					}
					if (val == SelectableRange.FIRST_IN_SET) {
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
					throw new ApplicationException("Unrecognised flag.");
			}

		}

		/// <summary>
		/// Adds a range from this set to the given list of integers.
		/// </summary>
		/// <param name="range">The instance of <see cref="SelectableRange"/>
		/// used to identify the indexes of nodes to add to the given list.</param>
		/// <param name="ivec">The list of integers where to add the selected range
		/// of indexes.</param>
		/// <remarks>
		/// IntegerList may be null if a list has not yet been allocated for 
		/// the range.
		/// </remarks>
		/// <returns>
		/// Returns the given <paramref name="ivec"/> integrated with the range
		/// of values identified by the given <pramref name="range"/> selector.
		/// </returns>
		private IntegerVector AddRange(SelectableRange range, IntegerVector ivec) {
			int r1, r2;

			// Select the range specified.
			byte start_flag = range.StartFlag;
			TObject start = range.Start;
			byte end_flag = range.EndFlag;
			TObject end = range.End;

			r1 = PositionOfRangePoint(start_flag, start);
			r2 = PositionOfRangePoint(end_flag, end);

			if (r2 < r1) {
				return ivec;
			}

			// Add the range to the set
			return AddRangeToSet(r1, r2, ivec);

		}

		/// <inheritdoc/>
		internal override IntegerVector SelectRange(SelectableRange range) {
			// If no items in the set return an empty set
			if (SetSize == 0) {
				return new IntegerVector(0);
			}

			IntegerVector ivec = AddRange(range, null);
			if (ivec == null) {
				return new IntegerVector(0);
			}

			return ivec;
		}

		/// <inheritdoc/>
		internal override IntegerVector SelectRange(SelectableRange[] ranges) {
			// If no items in the set return an empty set
			if (SetSize == 0) {
				return new IntegerVector(0);
			}

			IntegerVector ivec = null;
			for (int i = 0; i < ranges.Length; ++i) {
				SelectableRange range = ranges[i];
				ivec = AddRange(range, ivec);
			}

			if (ivec == null) {
				return new IntegerVector(0);
			}
			return ivec;

		}

	}
}