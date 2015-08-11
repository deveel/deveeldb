using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Cursors;

namespace Deveel.Data.Sql.Statements {
	public sealed class CloseStatement : SqlStatement {
		public CloseStatement(string cursorName) {
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
		}

		public string CursorName { get; private set; }

		protected override bool IsPreparable {
			get { return false; }
		}

		protected override ITable ExecuteStatement(IQueryContext context) {
			context.CloseCursor(CursorName);
			return FunctionTable.ResultTable(context, 0);
		}
	}
}
