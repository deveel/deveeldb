using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class ArrayNode : SqlNode, IExpressionNode {
		public IEnumerable<IExpressionNode> Items { get; private set; }

		protected override void OnNodeInit() {
			Items = ChildNodes.OfType<IExpressionNode>();
			base.OnNodeInit();
		}
	}
}
