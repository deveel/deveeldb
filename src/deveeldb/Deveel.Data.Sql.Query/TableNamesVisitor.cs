// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Query {
	class TableNamesVisitor : SqlExpressionVisitor {
		private IList<ObjectName> tableNames;

		public TableNamesVisitor(IList<ObjectName> tableNames) {
			this.tableNames = tableNames;
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (!value.IsNull && value.Value is SqlQueryObject &&
			    ((SqlQueryObject)value.Value).QueryPlan != null) {

				var queryObject = (SqlQueryObject) value.Value;
				var planNode = queryObject.QueryPlan;
				planNode.DiscoverTableNames(tableNames);
			}

			return base.VisitConstant(constant);
		}
	}
}