using System;

namespace Deveel.Data.Sql.Parser {
	class CloseCursorStatementNode : SqlNode, IStatementNode {
		public string CursorName { get; private set; }

		protected override void OnNodeInit() {
			CursorName = this.FindNode<IdentifierNode>().Text;
			base.OnNodeInit();
		}
	}
}
