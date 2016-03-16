using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class FetchIntoStatement : SqlStatement {
		public FetchIntoStatement(string cursorName, FetchDirection direction, SqlExpression offsetExpression, SqlExpression referenceExpression) {
			if (offsetExpression != null) {
				if (direction != FetchDirection.Absolute &&
					direction != FetchDirection.Relative)
					throw new ArgumentException("Cannot specify an offset for a FETCH that is not RELATIVE or ABSOLUTE");
			}

			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");
			if (referenceExpression == null)
				throw new ArgumentNullException("referenceExpression");

			CursorName = cursorName;
			Direction = direction;
			OffsetExpression = offsetExpression;
			ReferenceExpression = referenceExpression;
		}

		public string CursorName { get; private set; }

		public FetchDirection Direction { get; private set; }

		public SqlExpression OffsetExpression { get; private set; }

		public SqlExpression ReferenceExpression { get; private set; }
	}
}
