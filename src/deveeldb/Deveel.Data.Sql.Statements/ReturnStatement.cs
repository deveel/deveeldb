using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class ReturnStatement : SqlStatement {
		public ReturnStatement(SqlExpression returnValue) {
			if (returnValue == null)
				throw new ArgumentNullException("returnValue");

			ReturnValue = returnValue;
		}

		public SqlExpression ReturnValue { get; private set; }

		protected override IPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			throw new NotImplementedException();
		}
	}
}
