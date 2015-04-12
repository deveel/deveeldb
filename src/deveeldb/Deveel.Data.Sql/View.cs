using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class View : IDbObject {
		public View(ViewInfo viewInfo) {
			ViewInfo = viewInfo;
		}

		public ViewInfo ViewInfo { get; private set; }

		public IQueryPlanNode QueryPlan {
			get { return ViewInfo.QueryExpression.QueryPlan; }
		}

		ObjectName IDbObject.FullName {
			get { return ViewInfo.ViewName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.View; }
		}
	}
}
