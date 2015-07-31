using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class InsertValueNode : SqlNode {
		public IEnumerable<IExpressionNode> Values { get; private set;}

		protected override void OnNodeInit() {
			Values = this.FindNodes<IExpressionNode>();
			base.OnNodeInit();
		}
	}
}
