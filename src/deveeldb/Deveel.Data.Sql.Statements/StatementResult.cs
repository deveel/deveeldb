using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class StatementResult : IDisposable {
		~StatementResult() {
			Dispose(false);
		}

		public StatementResult(ITable result) {
			if (result == null)
				throw new ArgumentNullException("result");

			Result = result;
			Type = StatementResultType.Result;
		}

		public StatementResult(ICursor cursor) {
			if (cursor == null)
				throw new ArgumentNullException("cursor");

			Cursor = cursor;
			Type = StatementResultType.CursorRef;
		}

		public StatementResult(Exception error) {
			if (error == null)
				throw new ArgumentNullException("error");

			Type = StatementResultType.Exception;
			Error = error;
		}

		public ICursor Cursor { get; private set; }

		public ITable Result { get; private set; }

		public StatementResultType Type { get; private set; }

		public Exception Error { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Result != null)
					Result.Dispose();

				if (Cursor != null)
					Cursor.Dispose();
			}

			Cursor = null;
			Result = null;
		}
	}
}
