using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	class SequenceOfBlocksNode : SqlNode {
		private readonly ICollection<PlSqlBlockNode> blockNodes;

		public SequenceOfBlocksNode() {
			blockNodes = new List<PlSqlBlockNode>();
		}

		public IEnumerable<PlSqlBlockNode> BlockNodes {
			get { return blockNodes.AsEnumerable(); }
		}

		protected override void OnNodeInit() {
			ReadBlocks(ChildNodes);
			base.OnNodeInit();
		}

		private void ReadBlocks(IEnumerable<ISqlNode> nodes) {
			var blocks = nodes.OfType<PlSqlBlockNode>();
			foreach (var block in blocks) {
				blockNodes.Add(block);
			}
		}
	}
}
