using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	class LimitNode : SingleQueryPlanNode {
		public long Offset { get; private set; }

		public long Count { get; private set; }

		public LimitNode(IQueryPlanNode child, long offset, long count) 
			: base(child) {
			Offset = offset;
			Count = count;
		}

		public override ITable Evaluate(IRequest context) {
			var table = Child.Evaluate(context);
			return new LimitedTable(table, Offset, Count);
		}
	}
}
