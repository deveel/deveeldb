using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class PlSqlBlockNode : SqlNode {
		public string Label { get; private set; }

		public IEnumerable<IDeclareNode> Declarations { get; private set; }

		public IEnumerable<IStatementNode> Statements { get; private set; }

		protected override void OnNodeInit() {
			var label = this.FindNode<LabelNode>();
			if (label != null)
				Label = label.Text;

			Declarations = this.FindNodes<IDeclareNode>();
			Statements = this.FindNodes<IStatementNode>();

			base.OnNodeInit();
		}
	}
}
