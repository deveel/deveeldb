using System;
using System.Collections.Generic;

using Deveel.Diagnostics;

namespace Deveel.Data {
	public abstract partial class Table {
		/// <summary>
		/// Order the table by the given columns.
		/// </summary>
		/// <param name="columns">Column indices to order by the table.</param>
		/// <returns>
		/// Returns a table that is ordered by the given column numbers.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public Table OrderByColumns(int[] columns) {
			// Sort by the column list.
			Table table = this;
			for (int i = columns.Length - 1; i >= 0; --i) {
				table = table.OrderByColumn(columns[i], true);
			}

			// A nice post condition to check on.
			if (RowCount != table.RowCount)
				throw new ApplicationException("Internal Error, row count != sorted row count");

			return table;
		}

		/// <summary>
		/// Gets an ordered list of rows.
		/// </summary>
		/// <param name="columns">Column indices to order by the rows.</param>
		/// <returns>
		/// Returns an <see cref="IList{T}"/> that represents the list of 
		/// rows in this table in sorted order by the given <paramref name="columns"/>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the resultant table row count of the order differs from the 
		/// current table row count.
		/// </exception>
		public IList<int> OrderedRowList(int[] columns) {
			Table work = OrderByColumns(columns);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			int rowCount = RowCount;
			List<int> rowList = new List<int>(rowCount);
			IRowEnumerator e = work.GetRowEnumerator();
			while (e.MoveNext()) {
				rowList.Add(e.RowIndex);
			}

			work.SetToRowTableDomain(0, rowList, this);
			return rowList;
		}


		/// <summary>
		/// Gets a table ordered by the column identified by <paramref name="columnIndex"/>.
		/// </summary>
		/// <param name="columnIndex">Index of the column to sort by.</param>
		/// <param name="ascending">Flag indicating the order direction (set <b>true</b> for
		/// ascending direction, <b>false</b> for descending).</param>
		/// <returns>
		/// Returns a Table which is identical to this table, except it is sorted by
		/// the column identified by <paramref name="columnIndex"/>.
		/// </returns>
		public VirtualTable OrderByColumn(int columnIndex, bool ascending) {
			// Check the field can be sorted
			DataTableColumnInfo colInfo = GetColumnInfo(columnIndex);

			List<int> rows = new List<int>(SelectAll(columnIndex));

			// Reverse the list if we are not ascending
			if (ascending == false)
				rows.Reverse();

			// We now has an int[] array of rows from this table to make into a
			// new table.

			VirtualTable table = new VirtualTable(this);
			table.Set(this, rows);

#if DEBUG
			if (Logger.IsInterestedIn(LogLevel.Information))
				Logger.Info(this, table + " = " + this + ".OrderByColumn(" + columnIndex + ", " + ascending + ")");
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