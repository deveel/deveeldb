//  
//  Cursor.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// The class that represents a <c>CURSOR</c> to iterate through
	/// the results of a query to the database.
	/// </summary>
	/// <remarks>
	/// Cursors are named iterators that survive for the time of a transaction:
	/// when this is committed or rolledback, all the cursors created are disposed
	/// and they cannot be accessed anymore.
	/// </remarks>
	public sealed class Cursor : IEnumerator, IDisposable {
		internal Cursor(Transaction transaction, TableName name, IQueryPlanNode queryPlan, bool scrollable) {
			this.transaction = transaction;
			this.name = name;
			this.queryPlan = queryPlan;
			this.scrollable = scrollable;
		}

		/// <summary>
		/// A reference to the transaction contect that manages the cursor.
		/// </summary>
		private readonly Transaction transaction;

		/// <summary>
		/// The name of the cursor.
		/// </summary>
		private readonly TableName name;

		/// <summary>
		/// The current state of this cursor.
		/// </summary>
		private CursorState state;

		/// <summary>
		/// A flag that marks if the cursor is scrollable (can fetch
		/// more than the next element in the iteration).
		/// </summary>
		private readonly bool scrollable;

		/// <summary>
		/// The query that was used to generate the cursor.
		/// </summary>
		private IQueryPlanNode queryPlan;

		/// <summary>
		/// The result of the evaluation of the <see cref="queryPlan"/>
		/// of this cursor.
		/// </summary>
		private Table result;

		/// <summary>
		/// The index at which the cursor is currently set at.
		/// </summary>
		private int rowIndex = -1;

		/// <summary>
		/// The total number of columns in the table resulted from
		/// the evaluation of the query.
		/// </summary>
		private int columnCount;

		/// <summary>
		/// The maximum number of rows in the resulted table.
		/// </summary>
		private int rowCount;

		/// <summary>
		/// Gets the current <see cref="CursorState"/> of the cursor.
		/// </summary>
		/// <remarks>
		/// This property is set at the moment of <see cref="Open">opening</see>
		/// the cursor or at <see cref="Dispose">disposal</see>.
		/// </remarks>
		public CursorState State {
			get { return state; }
		}

		/// <summary>
		/// Gets the name of the cursor within the transaction.
		/// </summary>
		public TableName Name {
			get { return name; }
		}

		/// <summary>
		/// Builds a single row table for the row at the current index.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="Table"/> instance with a single row that
		/// encapsulates all the values of the current row in the cursor.
		/// </returns>
		private Table BuildRowTable() {
			try {
				TObject[] columns = new TObject[columnCount];
				for (int i = 0; i < columnCount; i++)
					columns[i] = result.GetCellContents(i, rowIndex);

				return new CursorRowTable(result, columns);
			} catch(Exception e) {
				state = CursorState.Broken;
				throw new InvalidOperationException(e.Message);
			}
		}

		private void CheckCursorOpen() {
			if (state != CursorState.Opened)
				throw new InvalidOperationException("The cursor '" + name + "' is not opened.");
		}

		private void CheckScrollable() {
			if (!IsScrollable)
				throw new InvalidOperationException("The cursor '" + name + "' is not scollable.");
		}

		internal void InternalDispose() {
			result = null;
			queryPlan = null;
		}

		/// <summary>
		/// Opens the cursor within the context given.
		/// </summary>
		/// <param name="context">The query context within the
		/// query that originated the cursor is evaluated.</param>
		/// <exception cref="InvalidOperationException">
		/// If the cursor is not in <see cref="CursorState.Closed"/> state.
		/// </exception>
		public void Open(IQueryContext context) {
			if (state != CursorState.Closed)
				throw new InvalidOperationException("Invalid cursor state.");

			try {
				result = queryPlan.Evaluate(context);
				columnCount = result.ColumnCount;
				rowCount = result.RowCount;
				state = CursorState.Opened;
			} catch(Exception e) {
				state = CursorState.Broken;
				throw new ApplicationException("Error while opening the cursor: " + e.Message);
			}
		}

		/// <summary>
		/// Closes the cursor and resets the position of the current
		/// row to the first one in the selection.
		/// </summary>
		/// <exception cref="InvalidOperationException">
		/// If the current state of the cursor is not opened.
		/// </exception>
		public void Close() {
			if (state != CursorState.Opened)
				throw new InvalidOperationException("Cursor '" + name + "' is not opened.");

			rowIndex = -1;
			state = CursorState.Closed;
		}

		/// <summary>
		/// Checks if the cursor has a row after the current index.
		/// </summary>
		/// <returns>
		/// Returns <c>true</c> if the cursor can advance to the next
		/// row, or <c>false</c> otherwise.
		/// </returns>
		public bool HasNext() {
			return rowIndex < rowCount;
		}

		/// <summary>
		/// Fetches the next row from the cursor and moves the current
		/// index forward by one position.
		/// </summary>
		/// <returns>
		/// Returns a single-row <see cref="Table"/> instance containing
		/// the next row from the cursor.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the cursor is not oepned or if the cursor index is after
		/// the last row within the enumeration.
		/// </exception>
		public Table FetchNext() {
			CheckCursorOpen();
			
			if (++rowIndex > rowCount)
				throw new InvalidOperationException("The cursor '" + name + "' is after the last row.");

			return BuildRowTable();
		}

		/// <summary>
		/// Checks if the cursor has a row before the current index.
		/// </summary>
		/// <returns>
		/// Returns <c>true</c> if the cursor is <see cref="IsScrollable"/> and can move 
		/// back to the previous row, or <c>false</c> otherwise.
		/// </returns>
		/// <seealso cref="IsScrollable"/>
		public bool HasPrevious() {
			return scrollable && rowIndex > 0;
		}

		/// <summary>
		/// Fetches the previous row from the cursor and moves the current
		/// index backward by one position.
		/// </summary>
		/// <remarks>
		/// This method only works if the instance of <see cref="Cursor"/>
		/// is <see cref="IsScrollable">scrollable</see>: otherwise it will
		/// throw an exception.
		/// </remarks>
		/// <returns>
		/// Returns a single-row <see cref="Table"/> instance containing
		/// the previous row from the cursor.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the cursor is not oepned or if the cursor index is before
		/// the first row within the enumeration. This is also thrown if
		/// the cursor is not <see cref="IsScrollable">scrollable</see>.
		/// </exception>
		/// <seealso cref="IsScrollable"/>
		/// <seealso cref="HasPrevious"/>
		public Table FetchPrevious() {
			// first we need to check the cursor is scrollable...
			CheckScrollable();

			CheckCursorOpen();

			if (--rowIndex < 0)
				throw new InvalidOperationException("The cursor '" + name + "' is before the first row.");

			return BuildRowTable();
		}

		/// <summary>
		/// Fetches the first row from the cursor and moves the current
		/// index to the beginning.
		/// </summary>
		/// <remarks>
		/// This method only works if the instance of <see cref="Cursor"/>
		/// is <see cref="IsScrollable">scrollable</see>: otherwise it will
		/// throw an exception.
		/// </remarks>
		/// <returns>
		/// Returns a single-row <see cref="Table"/> instance containing
		/// the first row from the cursor.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the cursor is not oepned or if the cursor is not 
		/// <see cref="IsScrollable">scrollable</see>.
		/// </exception>
		/// <seealso cref="IsScrollable"/>
		public Table FetchFirst() {
			CheckCursorOpen();

			CheckScrollable();

			rowIndex = 0;

			return BuildRowTable();
		}

		/// <summary>
		/// Fetches the last row from the cursor and moves the current
		/// index to the end.
		/// </summary>
		/// <remarks>
		/// This method only works if the instance of <see cref="Cursor"/>
		/// is <see cref="IsScrollable">scrollable</see>: otherwise it will
		/// throw an exception.
		/// </remarks>
		/// <returns>
		/// Returns a single-row <see cref="Table"/> instance containing
		/// the last row from the cursor.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the cursor is not oepned or if the cursor is not 
		/// <see cref="IsScrollable">scrollable</see>.
		/// </exception>
		/// <seealso cref="IsScrollable"/>
		public Table FetchLast() {
			CheckScrollable();

			rowIndex = rowCount - 1;

			return BuildRowTable();
		}

		/// <summary>
		/// Disposes the cursor removing it from the transaction
		/// context where it was declared.
		/// </summary>
		public void Dispose() {
			transaction.RemoveCursor(name);
		}

		#region CursorRowTable

		private class CursorRowTable : FilterTable {
			internal CursorRowTable(Table table, TObject[] columns)
				: base(table) {
				this.columns = columns;
			}

			private readonly TObject[] columns;

			public override int RowCount {
				get { return 1; }
			}

			public override TObject GetCellContents(int column, int row) {
				return columns[column];
			}
		}

		#endregion

		bool IEnumerator.MoveNext() {
			return HasNext();
		}

		void IEnumerator.Reset() {
			rowIndex = -1;
		}

		object IEnumerator.Current {
			get { return FetchNext(); }
		}

		/// <summary>
		/// Gets whether the cursor is scrollable or not.
		/// </summary>
		/// <remarks>
		/// A <c>scrollable</c> cursor can fetch more than the next 
		/// element in the iteration.
		/// </remarks>
		public bool IsScrollable {
			get { return scrollable; }
		}
	}
}