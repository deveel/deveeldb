using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class SetVariableStatementNode : SqlStatementNode {
		public IExpressionNode VariableReference { get; private set; }

		public IExpressionNode ValueExpression { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IExpressionNode) {
				if (VariableReference == null) {
					VariableReference = (IExpressionNode) node;
				} else {
					ValueExpression = (IExpressionNode) node;
				}
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			var varRefExp = ExpressionBuilder.Build(VariableReference);
			var valueExp = ExpressionBuilder.Build(ValueExpression);

			if (!(varRefExp is SqlVariableReferenceExpression) &&
				!(varRefExp is SqlReferenceExpression))
				throw new NotSupportedException("Only simple references are supported now.");

			builder.Objects.Add(new AssignVariableStatement(varRefExp, valueExp));
		}
	}
}