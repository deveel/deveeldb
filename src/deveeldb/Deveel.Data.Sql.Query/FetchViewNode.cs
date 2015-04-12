using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class FetchViewNode : IQueryPlanNode {
		public FetchViewNode(ObjectName viewName, ObjectName aliasName) {
			ViewName = viewName;
			AliasName = aliasName;
		}

		public ObjectName ViewName { get; private set; }

		public ObjectName AliasName { get; private set; }

		protected virtual IQueryPlanNode CreateChildNode(IQueryContext context) {
			return context.GetViewQueryPlan(ViewName);
		}

		public ITable Evaluate(IQueryContext context) {
			IQueryPlanNode node = CreateChildNode(context);
			var t = node.Evaluate(context);

			return AliasName != null ? new ReferenceTable(t, AliasName) : t;
		}
	}
}
