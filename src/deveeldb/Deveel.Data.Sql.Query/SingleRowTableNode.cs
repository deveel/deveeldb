using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class SingleRowTableNode : IQueryPlanNode {
		public ITable Evaluate(IQueryContext context) {
			return context.Session.Database.SingleRowTable;
		}
	}
}
