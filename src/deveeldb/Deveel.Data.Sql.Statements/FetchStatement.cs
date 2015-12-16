using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class FetchStatement : SqlStatement, IPreparableStatement, IPreparable {
		public FetchStatement(string cursorName, FetchDirection direction) {
			CursorName = cursorName;
			Direction = direction;
		}

		public string CursorName { get; private set; }

		public FetchDirection Direction { get; private set; }

		public SqlExpression PositionExpression { get; set; }

		public SqlExpression IntoReference { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		IStatement IPreparableStatement.Prepare(IRequest request) {
			throw new NotImplementedException();
		}
	}
}
