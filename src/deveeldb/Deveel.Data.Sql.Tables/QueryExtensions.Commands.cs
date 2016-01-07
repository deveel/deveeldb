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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Tables {
	public static partial class QueryExtensions {
		public static int UpdateTable(this IQuery context, ObjectName tableName, IQueryPlanNode queryPlan,
			IEnumerable<SqlAssignExpression> assignments, int limit) {
			var columnNames = assignments.Select(x => x.ReferenceExpression)
				.Cast<SqlReferenceExpression>()
				.Select(x => x.ReferenceName.Name).ToArray();

			if (!context.UserCanUpdateTable(tableName, columnNames))
				throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Update);

			if (!context.UserCanSelectFromPlan(queryPlan))
				throw new InvalidOperationException();

			var table = context.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			var updateSet = queryPlan.Evaluate(context);
			return table.Update(context, updateSet, assignments, limit);
		}

		public static void InsertIntoTable(this IQuery context, ObjectName tableName, IEnumerable<SqlAssignExpression> assignments) {
			var columnNames =
				assignments.Select(x => x.ReferenceExpression)
					.Cast<SqlReferenceExpression>()
					.Select(x => x.ReferenceName.Name).ToArray();
			if (!context.UserCanInsertIntoTable(tableName, columnNames))
				throw new MissingPrivilegesException(context.UserName(), tableName, Privileges.Insert);

			var table = context.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			var row = table.NewRow();
			foreach (var expression in assignments) {
				row.EvaluateAssignment(expression, context);
			}

			table.AddRow(row);
		}
	}
}
