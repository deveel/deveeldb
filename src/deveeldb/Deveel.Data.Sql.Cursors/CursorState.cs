using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorState : IDisposable {
		internal CursorState(Cursor cursor) {
			if (cursor == null)
				throw new ArgumentNullException("cursor");

			Cursor = cursor;
		}

		~CursorState() {
			Dispose(false);
		}

		public Cursor Cursor { get; private set; }

		public CursorStatus Status { get; private set; }

		public SqlExpression[] OpenArguments { get; private set; }

		internal ITable Result { get; private set; }

		private int CurrentOffset { get; set; }

		internal void Open(ITable result, SqlExpression[] args) {
			Status = CursorStatus.Open;
			Result = result;
			OpenArguments = args;
		}

		internal void Close() {
			Status = CursorStatus.Closed;
			OpenArguments = null;
		}

		internal Row FetchRowFrom(ITable table, FetchDirection direction, int offset) {
			int rowOffset;
			if (direction == FetchDirection.Next) {
				rowOffset = CurrentOffset + 1;
			} else if (direction == FetchDirection.Prior) {
				rowOffset = CurrentOffset - 1;
			} else if (direction == FetchDirection.First) {
				rowOffset = CurrentOffset = 0;
			} else if (direction == FetchDirection.Last) {
				rowOffset = table.RowCount;
			} else if (direction == FetchDirection.Absolute) {
				rowOffset = offset;
			} else if (direction == FetchDirection.Relative) {
				rowOffset = CurrentOffset + offset;
			} else {
				// Should never happen
				throw new InvalidOperationException();
			}

			if (rowOffset < 0 || rowOffset >= table.RowCount)
				throw new IndexOutOfRangeException(
					String.Format("The fetch offset '{0}' is smaller than zero or greater than the result set ({1}).", rowOffset,
						table.RowCount));

			CurrentOffset = rowOffset;
			Status = CursorStatus.Fetching;

			return table.GetRow(rowOffset);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Result != null)
					Result.Dispose();
			}

			Result = null;
			Cursor = null;
			Status = CursorStatus.Disposed;
		}
	}
}
