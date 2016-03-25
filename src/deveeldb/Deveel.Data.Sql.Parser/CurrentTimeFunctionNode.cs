using System;

namespace Deveel.Data.Sql.Parser {
	class CurrentTimeFunctionNode : SqlNode, IExpressionNode {
		public string FunctionName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				var key = (SqlKeyNode) node;
				if (String.Equals(key.Text, "CURRENT_TIME", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "TIME";
				} else if (String.Equals(key.Text, "CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "TIMESTAMP";
				} else if (String.Equals(key.Text, "CURRENT_DATE", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "DATE";
				} else {
					throw Error(String.Format("The keyword '{0}' is not allowed.", key.Text));
				}
			}

			return base.OnChildNode(node);
		}
	}
}
