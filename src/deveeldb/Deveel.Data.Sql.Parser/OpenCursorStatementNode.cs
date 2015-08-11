using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class OpenCursorStatementNode : SqlNode, IStatementNode {
		public string CursorName { get; private set; }

		public IEnumerable<IExpressionNode> Arguments { get; private set; }

		protected override void OnNodeInit() {
			CursorName = this.FindNode<IdentifierNode>().Text;
			Arguments = this.FindNodes<IExpressionNode>();
			base.OnNodeInit();
		}
	}
}
