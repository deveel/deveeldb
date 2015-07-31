using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class ValuesInsertNode : SqlNode {
		public IEnumerable<InsertValueNode> Values { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("insert_tuple")) {
				Values = node.FindNodes<InsertValueNode>();
			}

			return base.OnChildNode(node);
		}
	}
}
