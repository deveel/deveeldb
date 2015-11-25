using System;

namespace Deveel.Data.Sql.Statements.Blocks {
	class CreateTableBlock : Block {
		public CreateTableBlock(IRequest request) 
			: base(request) {
		}

		protected override void ExecuteBlock(BlockExecuteContext context) {
			base.ExecuteBlock(context);
		}
	}
}
