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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql {
	public sealed class ViewInfo : IObjectInfo {
		public ViewInfo(TableInfo tableInfo, SqlQueryExpression queryExpression, IQueryPlanNode queryPlan) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			TableInfo = tableInfo;
			QueryExpression = queryExpression;
			QueryPlan = queryPlan;
		}

		public TableInfo TableInfo { get; private set; }

		public ObjectName ViewName {
			get { return TableInfo.TableName; }
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public IQueryPlanNode QueryPlan { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.View; }
		}

		ObjectName IObjectInfo.FullName {
			get { return ViewName; }
		}
	}
}
