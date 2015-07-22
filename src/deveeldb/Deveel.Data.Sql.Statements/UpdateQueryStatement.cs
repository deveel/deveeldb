using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateQueryStatement : SqlStatement {
		public UpdateQueryStatement(string tableName, SqlQueryExpression sourceExpression, SqlExpression whereExpression) {
			TableName = tableName;
			SourceExpression = sourceExpression;
			WhereExpression = whereExpression;
		}

		public string TableName { get; private set; }

		public SqlExpression WhereExpression { get; private set; }

		public SqlQueryExpression SourceExpression { get; private set; }

		public int Limit { get; set; }

		public override StatementType StatementType {
			get { return StatementType.UpdateQuery; }
		}

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			throw new NotImplementedException();
		}
	}
}
