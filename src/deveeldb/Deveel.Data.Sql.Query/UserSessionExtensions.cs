// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

using Deveel.Data;

namespace Deveel.Data.Sql.Query {
	static class UserSessionExtensions {
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
