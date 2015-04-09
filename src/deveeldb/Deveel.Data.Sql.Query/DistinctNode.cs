using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class DistinctNode : SingleQueryPlanNode {
		public DistinctNode(QueryPlanNode child, ObjectName[] columnNames) 
			: base(child) {
			ColumnNames = columnNames;
		}

		public ObjectName[] ColumnNames { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var t = Child.Evaluate(context);
			int sz = ColumnNames.Length;
			int[] colMap = new int[sz];
			for (int i = 0; i < sz; ++i) {
				colMap[i] = t.IndexOfColumn(ColumnNames[i]);
			}

			return t.Distinct(colMap);
		}
	}
}
