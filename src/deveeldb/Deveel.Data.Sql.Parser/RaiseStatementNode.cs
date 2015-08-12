using System;

namespace Deveel.Data.Sql.Parser {
	class RaiseStatementNode : SqlNode, IStatementNode {
		public string ExceptionName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("exception_name_opt"))
				ExceptionName = node.FindNode<IdentifierNode>().Text;

			return base.OnChildNode(node);
		}
	}
}
