using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Cursors {
	class NativeCursorInfo : IObjectInfo {
		public NativeCursorInfo(IQueryPlanNode queryPlan) 
			: this(queryPlan, false) {
		}

		public NativeCursorInfo(IQueryPlanNode queryPlan, bool forUpdate) {
			QueryPlan = queryPlan;
			ForUpdate = forUpdate;
		}

		public IQueryPlanNode QueryPlan { get; private set; }

		public bool ForUpdate { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Cursor; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(NativeCursor.NativeCursorName); }
		}

		string IObjectInfo.Owner {
			get { return null; }
		}
	}
}
