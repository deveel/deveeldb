using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements.Blocks {
	class SelectBlock : Block {
		private readonly IQueryPlanNode queryPlan;

		public SelectBlock(IRequest request, IQueryPlanNode queryPlan) 
			: base(request) {
			this.queryPlan = queryPlan;
		}

		protected override void ExecuteBlock(BlockExecuteContext context) {
			var result = queryPlan.Evaluate(context.Query);
			context.SetResult(result);

			base.ExecuteBlock(context);
		}
	}
}
