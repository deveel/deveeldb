using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class SetInsertNode : SqlNode {
		public IEnumerable<InsertSetNode> Assignments { get; private set; }

		protected override void OnNodeInit() {
			Assignments = this.FindNodes<InsertSetNode>();
			base.OnNodeInit();
		}
	}
}
