using System;

namespace Deveel.Data.Sql.Parser {
	class UpdateStatementNode : SqlNode, IStatementNode {
		internal UpdateStatementNode() {
		}

		public SimpleUpdateNode SimpleUpdate { get; private set; }

		public QueryUpdateNode QueryUpdate { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SimpleUpdateNode) {
				SimpleUpdate = (SimpleUpdateNode) node;
			} else if (node is QueryUpdateNode) {
				QueryUpdate = (QueryUpdateNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}
