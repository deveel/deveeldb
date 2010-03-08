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

using Deveel.Data.Collections;

namespace Deveel.Data {
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
	sealed class INHelper {
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
		internal static IntegerVector In(Table table1, Table table2, int column1, int column2) {

			// First pick the the smallest and largest table.  We only want to iterate
			// through the smallest table.
			// NOTE: This optimisation can't be performed for the 'not_in' command.

			Table small_table;
			Table large_table;
			int small_column;
			int large_column;

			if (table1.RowCount < table2.RowCount) {
				small_table = table1;
				large_table = table2;

				small_column = column1;
				large_column = column2;

			} else {
				small_table = table2;
				large_table = table1;

				small_column = column2;
				large_column = column1;
			}

			// Iterate through the small table's column.  If we can find identical
			// cells in the large table's column, then we should include the row in our
			// final result.

			BlockIntegerList result_rows = new BlockIntegerList();
			IRowEnumerator e = small_table.GetRowEnumerator();
			Operator EQUALSOP = Operator.Get("=");

			while (e.MoveNext()) {

				int small_row_index = e.RowIndex;
				TObject cell =
						   small_table.GetCellContents(small_column, small_row_index);

				IntegerVector selected_set =
						   large_table.SelectRows(large_column, EQUALSOP, cell);

				// We've found cells that are IN both columns,

				if (selected_set.Count > 0) {

					// If the large table is what our result table will be based on, append
					// the rows selected to our result set.  Otherwise add the index of
					// our small table.  This only works because we are performing an
					// EQUALS operation.

					if (large_table == table1) {
						// Only allow unique rows into the table set.
						int sz = selected_set.Count;
						bool rs = true;
						for (int i = 0; rs == true && i < sz; ++i) {
							rs = result_rows.UniqueInsertSort(selected_set[i]);
						}
					} else {
						// Don't bother adding in sorted order because it's not important.
						result_rows.Add(small_row_index);
					}
				}

			}

			return new IntegerVector(result_rows);

		}

		/// <summary>
		/// A multi-column version of <c>IN</c>.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1_cols"></param>
		/// <param name="t2_cols"></param>
		/// <returns></returns>
		internal static IntegerVector In(Table table1, Table table2, int[] t1_cols, int[] t2_cols) {
			if (t1_cols.Length > 1) {
				throw new ApplicationException("Multi-column 'in' not supported.");
			}
			return In(table1, table2, t1_cols[0], t2_cols[0]);
		}

		/// <summary>
		/// This implements the <c>not in</c> command.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="col1"></param>
		/// <param name="col2"></param>
		/// <remarks>
		/// <b>Issue</b>: This will be less efficient than <see cref="In(Deveel.Data.Table,Deveel.Data.Table,int,int)">in</see> 
		/// if <paramref name="table1"/> has many rows and <paramref name="table2"/> has few rows.
		/// </remarks>
		/// <returns></returns>
		internal static IntegerVector NotIn(Table table1, Table table2, int col1, int col2) {

			// Handle trivial cases
			int t2_row_count = table2.RowCount;
			if (t2_row_count == 0) {
				// No rows so include all rows.
				return table1.SelectAll(col1);
			} else if (t2_row_count == 1) {
				// 1 row so select all from table1 that doesn't equal the value.
				IRowEnumerator en = table2.GetRowEnumerator();
				TObject cell = table2.GetCellContents(col2, en.RowIndex);
				return table1.SelectRows(col1, Operator.Get("<>"), cell);
			}

			// Iterate through table1's column.  If we can find identical cell in the
			// tables's column, then we should not include the row in our final
			// result.
			IntegerVector result_rows = new IntegerVector();
			IRowEnumerator e = table1.GetRowEnumerator();
			Operator EQUALSOP = Operator.Get("=");

			while (e.MoveNext()) {

				int row_index = e.RowIndex;
				TObject cell = table1.GetCellContents(col1, row_index);

				IntegerVector selected_set =
									  table2.SelectRows(col2, Operator.Get("="), cell);

				// We've found a row in table1 that doesn't have an identical cell in
				// table2, so we should include it in the result.

				if (selected_set.Count <= 0) {
					result_rows.AddInt(row_index);
				}

			}

			return result_rows;
		}

		/// <summary>
		/// A multi-column version of NOT IN.
		/// </summary>
		/// <param name="table1"></param>
		/// <param name="table2"></param>
		/// <param name="t1_cols"></param>
		/// <param name="t2_cols"></param>
		/// <returns></returns>
		internal static IntegerVector NotIn(Table table1, Table table2, int[] t1_cols, int[] t2_cols) {
			if (t1_cols.Length > 1) {
				throw new ApplicationException("Multi-column 'not in' not supported.");
			}
			return NotIn(table1, table2, t1_cols[0], t2_cols[0]);
		}
	}
}