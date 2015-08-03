using System;

namespace Deveel.Data.Sql.Parser {
	class SetAccountStatusNode : SqlNode, IAlterUserActionNode {
		public string Status { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode &&
				(node.NodeName.Equals("LOCK", StringComparison.OrdinalIgnoreCase) ||
				node.NodeName.Equals("UNLOCK", StringComparison.OrdinalIgnoreCase))) {
				Status = node.NodeName;
			}

			return base.OnChildNode(node);
		}
	}
}
