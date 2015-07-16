using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class AlterTableNode : SqlNode {
		internal AlterTableNode() {
		}

		public ObjectNameNode TableName { get; private set; }

		public IEnumerable<AlterTableActionNode> Actions { get; private set; }

		public CreateTableNode CreateTable { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			return base.OnChildNode(node);
		}
	}
}
