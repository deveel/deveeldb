using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class DropViewStatementNode : SqlNode, IStatementNode {
		public IEnumerable<string> ViewNames { get; private set; }

		public bool IfExists { get; private set; }

		protected override void OnNodeInit() {
			ViewNames = this.FindNodes<ObjectNameNode>().Select(x => x.Name);
			var ifExistsOpt = this.FindByName("if_exists_opt");
			if (ifExistsOpt != null && ifExistsOpt.ChildNodes.Any())
				IfExists = true;

			base.OnNodeInit();
		}
	}
}
