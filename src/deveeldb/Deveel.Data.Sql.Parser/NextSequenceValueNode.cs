using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class NextSequenceValueNode : SqlNode, IExpressionNode {
		public ObjectNameNode SequenceName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode)
				SequenceName = (ObjectNameNode) node;

			return base.OnChildNode(node);
		}
	}
}
