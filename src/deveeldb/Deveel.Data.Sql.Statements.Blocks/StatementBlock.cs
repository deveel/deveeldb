using System;

namespace Deveel.Data.Sql.Statements.Blocks {
	public abstract class StatementBlock : Block {
		protected StatementBlock(Data.Query query) 
			: base(query) {
		}

		protected StatementBlock(StatementBlock statement)
			: base(statement) {
		}

		protected StatementBlock FindNamedBlock(string blockName) {
			if (this is INamedBlock &&
			    ((INamedBlock) this).Name == blockName)
				return this;

			IBlock root = this;
			while (root != null) {
				if (root.ParentSource is IQuery)
					break;
				
				root = root.Parent;
			}

			if (root == null)
				return null;

			var next = root.Next as StatementBlock;
			while (next != null) {
				var namedBlock = next.FindNamedBlock(blockName);
				if (namedBlock != null)
					return namedBlock;

				next = next.Next as StatementBlock;
			}

			return null;
		}

		protected abstract void Execute(BlockExecuteContext context);

		protected override void ExecuteBlock(BlockExecuteContext context) {
			Execute(context);
		}
	}
}
