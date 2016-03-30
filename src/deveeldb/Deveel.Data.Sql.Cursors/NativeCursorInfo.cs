using System;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Cursors {
	class NativeCursorInfo : IObjectInfo {
		public NativeCursorInfo(IQueryPlanNode queryPlan) {
			QueryPlan = queryPlan;
		}

		public IQueryPlanNode QueryPlan { get; private set; }

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
