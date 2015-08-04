using System;

namespace Deveel.Data.Sql.Parser {
	class DeclareVariableNode : SqlNode, IDeclareNode {
		public string VariableName { get; private set; }

		public DataTypeNode Type { get; private set; }

		public bool IsConstant { get; private set; }

		public bool IsNotNull { get; private set; }

		public IExpressionNode DefaultExpression { get; private set; }

		public IExpressionNode AssignExpression { get; private set; }
	}
}
