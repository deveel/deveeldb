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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class UpdateBuilder {
		public static UpdateStatement Build(PlSqlParser.UpdateStatementContext context) {
			var tableName = Name.Object(context.objectName());
			var setClause = context.updateSetClause();
			var limitClause = context.updateLimitClause();

			if (setClause != null) {
				var assignments = new List<SqlColumnAssignment>();
				var whereClause = context.whereClause();
				int limit = -1;

				if (limitClause != null)
					limit = Number.PositiveInteger(limitClause.numeric()) ?? -1;

				SqlExpression whereExpression = null;
				if (whereClause != null)
					whereExpression = Expression.Build(whereClause.conditionWrapper());

				if (setClause.VALUE() != null) {
					var columnName = Name.Simple(setClause.columnName());
					var value = Expression.Build(setClause.expression());

					assignments.Add(new SqlColumnAssignment(columnName, value));
				} else {
					var pairs = setClause.columnBasedUpdateClause().Select(x => new {
						columnName = Name.Simple(x.columnName()),
						value = Expression.Build(x.expression())
					});

					assignments = pairs.Select(x => new SqlColumnAssignment(x.columnName, x.value)).ToList();
				}

				return new UpdateStatement(tableName, whereExpression, assignments) {
					Limit = limit
				};
			}
			if (context.updateFromClause() != null) {
				var query = Subquery.Form(context.updateFromClause().subquery());
			}

			throw new NotSupportedException();
		}
	}
}
