using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class DropTableStatementNode : SqlNode, IStatementNode {
		public IEnumerable<string> TableNames { get; private set; }

		public bool IfExists { get; private set; }

		protected override void OnNodeInit() {
			var tableNames = this.FindNodes<ObjectNameNode>();
			TableNames = tableNames.Select(x => x.Name);

			var ifExistsOpt = this.FindByName("if_exists_opt");
			if (ifExistsOpt != null && ifExistsOpt.ChildNodes.Any())
				IfExists = true;

			base.OnNodeInit();
		}
	}
}
