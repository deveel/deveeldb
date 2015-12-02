using System;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class CreateSequenceNode : SqlNode, IStatementNode {
		public string SequenceName { get; private set; }

		public IExpressionNode IncrementBy { get; private set; }

		public IExpressionNode MinValue { get; private set; }

		public IExpressionNode MaxValue { get; private set; }

		public IExpressionNode StartWith { get; private set; }

		public bool Cycle { get; private set; }

		public IExpressionNode Cache { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				SequenceName = ((ObjectNameNode) node).Name;
			} else if (node.NodeName.Equals("start_opt")) {
				StartWith = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("increment_opt")) {
				IncrementBy = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("minvalue_opt")) {
				MinValue = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("maxvalue_opt")) {
				MaxValue = node.FindNode<IExpressionNode>();
			} else if (node.NodeName.Equals("cycle_opt")) {
				Cycle = node.ChildNodes.Any();
			} else if (node.NodeName.Equals("cache_opt")) {
				Cache = node.FindNode<IExpressionNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
