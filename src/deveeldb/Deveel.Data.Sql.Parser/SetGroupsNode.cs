using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class SetGroupsNode : SqlNode, IAlterUserActionNode {
		public IEnumerable<IExpressionNode> Groups { get; private set; }

		protected override void OnNodeInit() {
			Groups = this.FindNodes<IExpressionNode>();
			base.OnNodeInit();
		}
	}
}
