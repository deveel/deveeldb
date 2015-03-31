using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class View : IDbObject {
		public View(TableInfo tableInfo, IQueryPlanNode queryPlan) {
			TableInfo = tableInfo;
			QueryPlan = queryPlan;
		}

		public TableInfo TableInfo { get; private set; }

		public IQueryPlanNode QueryPlan { get; private set; }

		ObjectName IDbObject.FullName {
			get { return TableInfo.TableName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.View; }
		}
	}
}
