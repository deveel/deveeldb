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

using Deveel.Data.Index;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Provides methods for performing the query table command <i>in</i> and 
	/// <i>not in</i>.
	/// </summary>
	/// <remarks>
	/// The utilities finds a match between one of the columns in two tables. 
	/// If match between a cell in one column is also found in the column of 
	/// the other table, the row is included in the resultant table (or 
	/// discluded (is that a word?) for 'not in').
	/// </remarks>
	static class InHelper {
		/// <summary>
		/// This implements the <c>in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="column1"></param>
		/// <param name="column2"></param>
		/// <returns>
		/// Returns the rows selected from <paramref name="table1"/>.
		/// </returns>
		public static IList<int> In(Table table1, Table table2, int column1, int column2) {
			// First pick the the smallest and largest table.  We only want to iterate
			// through the smallest table.
			// NOTE: This optimisation can't be performed for the 'not_in' command.

			Table smallTable;
			Table largeTable;
			int smallColumn;
			int largeColumn;

			if (table1.RowCount < table2.RowCount) {
				smallTable = table1;
				largeTable = table2;

				smallColumn = column1;
				largeColumn = column2;

			} else {
				smallTable = table2;
				largeTable = table1;

				smallColumn = column2;
				largeColumn = column1;
			}

			// Iterate through the small table's column.  If we can find identical
			// cells in the large table's column, then we should include the row in our
			// final result.

			BlockIndex resultRows = new BlockIndex();
			IRowEnumerator e = smallTable.GetRowEnumerator();
			Operator op = Operator.Get("=");

			while (e.MoveNext()) {
				int smallRowIndex = e.RowIndex;
				TObject cell = smallTable.GetCell(smallColumn, smallRowIndex);

				IList<int> selectedSet = largeTable.SelectRows(largeColumn, op, cell);

				// We've found cells that are IN both columns,

				if (selectedSet.Count > 0) {
					// If the large table is what our result table will be based on, append
					// the rows selected to our result set.  Otherwise add the index of
					// our small table.  This only works because we are performing an
					// EQUALS operation.

					if (largeTable == table1) {
						// Only allow unique rows into the table set.
						int sz = selectedSet.Count;
						bool rs = true;
						for (int i = 0; rs && i < sz; ++i) {
							rs = resultRows.UniqueInsertSort(selectedSet[i]);
						}
					} else {
						// Don't bother adding in sorted order because it's not important.
						resultRows.Add(smallRowIndex);
					}
				}
			}

			return new List<int>(resultRows);
		}

		/// <summary>
		/// A multi-column version of <c>IN</c>.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IList<int> In(Table table1, Table table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'in' not supported yet.");

			return In(table1, table2, t1Cols[0], t2Cols[0]);
		}

		/// <summary>
		/// This implements the <c>not in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// <b>Issue</b>: This will be less efficient than <see cref="In(Table,Table,int,int)">in</see> 
		/// if <paramref name="table1"/> has many rows and <paramref name="table2"/> has few rows.
		/// </remarks>
		/// <returns></returns>
		public static IList<int> NotIn(Table table1, Table table2, int col1, int col2) {
			// Handle trivial cases
			int t2_row_count = table2.RowCount;
			if (t2_row_count == 0)
				// No rows so include all rows.
				return table1.SelectAll(col1);

			if (t2_row_count == 1) {
				// 1 row so select all from table1 that doesn't equal the value.
				IRowEnumerator en = table2.GetRowEnumerator();
				if (!en.MoveNext())
					throw new InvalidOperationException("Cannot iterate through table rows.");

				TObject cell = table2.GetCell(col2, en.RowIndex);
				return table1.SelectRows(col1, Operator.Get("<>"), cell);
			}

			// Iterate through table1's column.  If we can find identical cell in the
			// tables's column, then we should not include the row in our final
			// result.
			List<int> resultRows = new List<int>();
			IRowEnumerator e = table1.GetRowEnumerator();

			while (e.MoveNext()) {
				int rowIndex = e.RowIndex;
				TObject cell = table1.GetCell(col1, rowIndex);

				IList<int> selectedSet = table2.SelectRows(col2, Operator.Equal, cell);

				// We've found a row in table1 that doesn't have an identical cell in
				// table2, so we should include it in the result.

				if (selectedSet.Count <= 0)
					resultRows.Add(rowIndex);
			}

			return resultRows;
		}

		/// <summary>
		/// A multi-column version of NOT IN.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1Cols"></param>
		/// <param name="t2Cols"></param>
		/// <returns></returns>
		public static IList<int> NotIn(Table table1, Table table2, int[] t1Cols, int[] t2Cols) {
			if (t1Cols.Length > 1)
				throw new NotSupportedException("Multi-column 'not in' not supported yet.");

			return NotIn(table1, table2, t1Cols[0], t2Cols[0]);
		}
	}
}