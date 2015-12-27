using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DeleteStatementNode : SqlStatementNode {
		public string TableName { get; private set; }

		public IExpressionNode WhereExpression { get; private set; }

		public int Limit { get; private set; }

		public bool FromCursor { get; private set; }

		public string CursorName { get; private set; }

		public bool Current { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var tableName = ObjectName.Parse(TableName);

			if (FromCursor) {
				var whereExp = ExpressionBuilder.Build(WhereExpression);
				builder.Objects.Add(new DeleteStatement(tableName, whereExp, Limit));
			} else {
				throw new NotSupportedException();
			}
		}
	}
}