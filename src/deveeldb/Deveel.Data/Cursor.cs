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
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
using Deveel.Data.Sql;

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
		internal Cursor(ICursorContext cursorContext, TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			this.cursorContext = cursorContext;
			this.name = name;
			this.queryPlan = queryPlan;

			if ((attributes & CursorAttributes.ReadOnly) != 0 &&
				(attributes & CursorAttributes.Update) != 0)
				throw new ArgumentException("A read-only cursor cannot be marked also for update.");

			if (((attributes & CursorAttributes.Insensitive) != 0 ||
				(attributes & CursorAttributes.Scrollable) != 0) &&
				(attributes & CursorAttributes.Update) != 0)
				throw new ArgumentException("A scrollable or insensitive cursor cannot be updateable.");

			this.attributes = attributes;

			cursorContext.OnCursorCreated(this);
		}

		/// <summary>
		/// The name of the cursor.
		/// </summary>
		private readonly TableName name;

		/// <summary>
		/// The current state of this cursor.
		/// </summary>
		private CursorState state = CursorState.Closed;

		/// <summary>
		/// The attributes defined for the cursor.
		/// </summary>
		private readonly CursorAttributes attributes;

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

		private TableExpressionFromSet fromSet;

		/// <summary>
		/// A reference to the context that manages the cursor.
		/// </summary>
		private readonly ICursorContext cursorContext;

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
		/// Gets the current index to the row within thwe
		/// </summary>
		internal int RowIndex {
			get { return rowIndex; }
		}

		/// <summary>
		/// Gets the table which is the result of the evaluation of the
		/// query that generated the cursor.
		/// </summary>
		internal Table SelectedTable {
			get { return result; }
		}

		internal TableExpressionFromSet From {
			get { return fromSet; }
			set { fromSet = value; }
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
					columns[i] = result.GetCell(i, rowIndex);

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

		public Table Fetch(FetchOrientation orientation) {
			return Fetch(orientation, -1);
		}

		public Table Fetch(FetchOrientation orientation, int offset) {
			CheckCursorOpen();

			if (offset != -1 &&
				(orientation != FetchOrientation.Relative &&
				orientation != FetchOrientation.Absolute))
				throw new ArgumentException("Cannot specifiy an offset for the fetch that is not absolute or relative.");

			if (orientation == FetchOrientation.Next && ++rowIndex > rowCount)
				throw new InvalidOperationException("The cursor '" + name + "' is after the last row.");
			if (orientation == FetchOrientation.Prior) {
				CheckScrollable();

				if (--rowIndex < 0)
					throw new InvalidOperationException("Cannot fetch before the first row.");
			} else if (orientation == FetchOrientation.Last) {
				rowIndex = rowCount;
			} else if (orientation == FetchOrientation.First) {
				CheckScrollable();

				rowIndex = 0;
			} else if (orientation == FetchOrientation.Absolute) {
				if (offset < rowIndex && !IsScrollable)
					throw new InvalidOperationException("The absolute position is before the current row and the cursor " +
					                                    "is not scrollable.");
				if (offset >= rowCount)
					throw new InvalidOperationException("The absolute position is after the last row of the cursor.");
				if (offset < 0)
					throw new InvalidOperationException("The absolute position if before the first row of the cursor.");

				rowIndex = offset;
			} else if (orientation == FetchOrientation.Relative) {
				int pos = rowIndex + offset;

				if (pos < rowIndex && !IsScrollable)
					throw new InvalidOperationException("The absolute position is before the current row and the cursor " +
														"is not scrollable.");

				if (pos >= rowCount)
					throw new InvalidOperationException("The relative position if after the last row of the cursor.");
				if (pos < 0)
					throw new InvalidOperationException("The relative position is before the first row of the cursor.");

				rowIndex = pos;
			}
			
			return BuildRowTable();
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
			return Fetch(FetchOrientation.Next);
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
			return IsScrollable && rowIndex > 0;
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
		public Table FetchPrior() {
			return Fetch(FetchOrientation.Prior);
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
			return Fetch(FetchOrientation.First);
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
			return Fetch(FetchOrientation.Last);
		}

		public Table FetchRelative(int offset) {
			return Fetch(FetchOrientation.Relative, offset);
		}

		public Table FetchAbsolute(int offset) {
			return Fetch(FetchOrientation.Absolute, offset);
		}

		public Table FetchInto(FetchOrientation orientation, int offset, IQueryContext context, SelectIntoClause into) {
			Table table = Fetch(orientation, offset);
			return into.SelectInto(context, table);
		}

		public Table FetchInto(FetchOrientation orientation, IQueryContext context, SelectIntoClause into) {
			return FetchInto(orientation, -1, context, into);
		}

		public Table FetchNextInto(IQueryContext context, SelectIntoClause into) {
			return FetchInto(FetchOrientation.Next, context, into);
		}

		public Table FetchPriorInto(IQueryContext context, SelectIntoClause into) {
			return FetchInto(FetchOrientation.Prior, context, into);
		}

		public Table FetchFirstInto(IQueryContext context, SelectIntoClause into) {
			return FetchInto(FetchOrientation.First, -1, context, into);
		}

		public Table FetchLastInto(IQueryContext context, SelectIntoClause into) {
			return FetchInto(FetchOrientation.Last, context, into);
		}

		public Table FetchRelativeInto(IQueryContext context, int offset, SelectIntoClause into) {
			return FetchInto(FetchOrientation.Relative, offset, context, into);
		}

		public Table FetchAbsoluteInto(IQueryContext context, int offset, SelectIntoClause into) {
			return FetchInto(FetchOrientation.Absolute, offset, context, into);
		}

		/// <summary>
		/// Disposes the cursor removing it from the transaction
		/// context where it was declared.
		/// </summary>
		public void Dispose() {
			cursorContext.OnCursorDisposing(this);
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

			public override TObject GetCell(int column, int row) {
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
			get { return (attributes & CursorAttributes.Scrollable) != 0; }
		}

		public bool IsUpdate {
			get { return (attributes & CursorAttributes.Update) != 0; }
		}

		public bool IsReadOnly {
			get { return (attributes & CursorAttributes.ReadOnly) != 0; }
		}

		public CursorAttributes Attributes {
			get { return attributes; }
		}
	}
}