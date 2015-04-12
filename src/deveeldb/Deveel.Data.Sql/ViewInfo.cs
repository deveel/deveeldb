using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql {
	public sealed class ViewInfo : IObjectInfo {
		public ViewInfo(TableInfo tableInfo, SqlPreparedQueryExpression queryExpression) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			TableInfo = tableInfo;
			QueryExpression = queryExpression;
		}

		public ViewInfo(TableInfo tableInfo, IQueryPlanNode queryPlan)
			: this(tableInfo, new SqlPreparedQueryExpression(queryPlan)) {
		}

		public TableInfo TableInfo { get; private set; }

		public ObjectName ViewName {
			get { return TableInfo.TableName; }
		}

		public SqlPreparedQueryExpression QueryExpression { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.View; }
		}

		ObjectName IObjectInfo.FullName {
			get { return ViewName; }
		}
	}
}
