using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Statements.Blocks {
	public abstract class BranchBlock : Block {
		private IList<IBlock> blocks;
		 
		protected BranchBlock(IRequest request) 
			: base(request) {
			blocks = new List<IBlock>();
		}

		public IEnumerable<IBlock> Block {
			get { return blocks.AsEnumerable(); }
		}

		public void Append(IBlock block) {
			if (block == null)
				throw new ArgumentNullException("block");

			if (!Equals(block.Parent, this))
				throw new ArgumentException("The parent of the block must be this block.");

			blocks.Add(block);
		}

		protected override void ExecuteBlock(BlockExecuteContext context) {
			foreach (var block in blocks) {
				block.Execute(context);
			}

			base.ExecuteBlock(context);
		}
	}
}
