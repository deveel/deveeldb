// 
//  Copyright 2010-2011  Deveel
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
using Deveel.Diagnostics;

namespace Deveel.Data {
	public abstract partial class Table {
		/// <summary>
		/// Order the table by the given columns.
		/// </summary>
		/// <param name="columnIndexes">Column indices to order by the table.</param>
		/// <returns>
		/// Returns a table that is ordered by the given column numbers.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public Table OrderByColumns(int[] columnIndexes) {
			// Sort by the column list.
			Table work = this;
			for (int i = columnIndexes.Length - 1; i >= 0; --i)
				work = work.OrderByColumn(columnIndexes[i], true);

			// A nice post condition to check on.
			if (RowCount != work.RowCount)
				throw new ApplicationException("Internal Error, row count != sorted row count");

			return work;
		}

		/// <summary>
		/// Gets an ordered list of rows.
		/// </summary>
		/// <param name="columnIndexes">Column indices to order by the rows.</param>
		/// <returns>
		/// Returns an <see cref="IntegerVector"/> that represents the list of 
		/// rows in this table in sorted order by the given <paramref name="columnIndexes"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public IntegerVector OrderedRowList(int[] columnIndexes) {
			Table work = OrderByColumns(columnIndexes);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			int rowCount = RowCount;
			IntegerVector rowList = new IntegerVector(rowCount);
			IRowEnumerator e = work.GetRowEnumerator();
			while (e.MoveNext()) {
				rowList.AddInt(e.RowIndex);
			}

			work.SetToRowTableDomain(0, rowList, this);
			return rowList;
		}


		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="col_index"/>.
		/// </summary>
		/// <param name="col_index">Index of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="col_index"/>.
		/// </returns>
		public VirtualTable OrderByColumn(int col_index, bool ascending) {
			IntegerVector rows = SelectAll(col_index);

			// Reverse the list if we are not ascending
			if (ascending == false)
				rows.Reverse();

			// We now has an int[] array of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

#if DEBUG
			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this,
							table + " = " + this + ".OrderByColumn(" +
							col_index + ", " + ascending + ")");
			}
#endif

			return table;
		}

		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="column"/>.
		/// </summary>
		/// <param name="column">Name of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="column"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the given column name was not found.
		/// </exception>
		public VirtualTable OrderByColumn(VariableName column, bool ascending) {
			int colIndex = FindFieldName(column);
			if (colIndex == -1)
				throw new ApplicationException("Unknown column in 'OrderByColumn' ( " + column + " )");

			return OrderByColumn(colIndex, ascending);
		}

		public VirtualTable OrderByColumn(VariableName column) {
			return OrderByColumn(column, true);
		} 
	}
}