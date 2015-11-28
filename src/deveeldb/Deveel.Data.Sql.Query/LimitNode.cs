using System;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class LimitNode : SingleQueryPlanNode {
		public long Offset { get; private set; }

		public long Count { get; private set; }

		public LimitNode(IQueryPlanNode child, long offset, long count) 
			: base(child) {
			Offset = offset;
			Count = count;
		}

		private LimitNode(ObjectData data)
			: base(data) {
			Offset = data.GetInt64("Offset");
			Count = data.GetInt64("Count");
		}

		public override ITable Evaluate(IRequest context) {
			var table = Child.Evaluate(context);
			return new LimitedTable(table, Offset, Count);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("Offset", Offset);
			data.SetValue("Count", Count);
		}
	}
}
