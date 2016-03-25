using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class AssignStatementNode : SqlStatementNode {
		public string VariableName { get; private set; }

		public IExpressionNode Value { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				VariableName = ((IdentifierNode) node).Text;
			} else if (node is IExpressionNode) {
				Value = (IExpressionNode) node;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			var variable = VariableName;
			if (String.IsNullOrEmpty(VariableName))
				throw Error("The variable name was not present");

			if (variable[0] == ':')
				variable = variable.Substring(1);

			if (String.IsNullOrEmpty(variable))
				throw Error("The name of the variable is invalid: cannot specify only ':' pointer.");

			var varRef = SqlExpression.VariableReference(variable);
			var value = ExpressionBuilder.Build(Value);

			builder.AddObject(new AssignVariableStatement(varRef, value));
		}
	}
}
