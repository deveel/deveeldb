using System;

namespace Deveel.Data.Sql.Parser {
	class CreateUserStatementNode : SqlNode, IStatementNode {
		public string UserName { get; private set; }

		public IUserIdentificatorNode Identificator { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is IdentifierNode) {
				UserName = ((IdentifierNode) node).Text;
			} else if (node is IUserIdentificatorNode) {
				Identificator = (IUserIdentificatorNode) node;
			}

			return base.OnChildNode(node);
		}
	}
}
