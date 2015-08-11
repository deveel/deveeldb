using System;

namespace Deveel.Data.Sql.Parser {
	class CursorParameterNode : SqlNode {
		public string ParameterName { get; private set; }

		public DataTypeNode ParameterType { get; private set; }

		protected override void OnNodeInit() {
			ParameterName = this.FindNode<IdentifierNode>().Text;
			ParameterType = this.FindNode<DataTypeNode>();

			base.OnNodeInit();
		}
	}
}
