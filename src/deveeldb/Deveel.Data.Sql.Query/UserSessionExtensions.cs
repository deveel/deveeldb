using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	public static class UserSessionExtensions {
		public static ITableQueryInfo GetTableQueryInfo(this IUserSession session, ObjectName tableName, ObjectName alias) {
			var tableInfo = session.GetTableInfo(tableName);
			if (alias != null) {
				tableInfo = tableInfo.Alias(alias);
			}

			return new TableQueryInfo(session, tableInfo, tableName, alias);
		}

		public static IQueryPlanNode CreateQueryPlan(this IUserSession session, ObjectName tableName, ObjectName aliasedName) {
			string tableType = session.GetTableType(tableName);
			if (tableType.Equals(TableTypes.View))
				return new FetchViewNode(tableName, aliasedName);

			return new FetchTableNode(tableName, aliasedName);
		}

		#region TableQueryInfo

		class TableQueryInfo : ITableQueryInfo {
			public TableQueryInfo(IUserSession session, TableInfo tableInfo, ObjectName tableName, ObjectName aliasName) {
				Session = session;
				TableInfo = tableInfo;
				TableName = tableName;
				AliasName = aliasName;
			}

			public IUserSession Session { get; private set; }

			public TableInfo TableInfo { get; private set; }

			public ObjectName TableName { get; set; }

			public ObjectName AliasName { get; set; }

			public IQueryPlanNode QueryPlanNode {
				get { return Session.CreateQueryPlan(TableName, AliasName); }
			}
		}

		#endregion

	}
}
