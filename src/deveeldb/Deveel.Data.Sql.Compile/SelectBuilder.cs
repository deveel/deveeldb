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
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class SelectBuilder {
		public static SqlStatement Build(PlSqlParser.SelectStatementContext context) {
			IntoClause into;
			var query = Subquery.Form(context.subquery(), out into);

			if (into != null) {
				SqlExpression reference;
				if (into.TableName != null) {
					reference = SqlExpression.Reference(into.TableName);
				} else {
					var vars = into.Variables;
					reference = SqlExpression.Tuple(vars.Select(SqlExpression.VariableReference).Cast<SqlExpression>().ToArray());
				}

				return new SelectIntoStatement(query, reference);
			}

			var statement = new SelectStatement(query);

			var orderBy = context.orderByClause();
			var forUpdate = context.forUpdateClause();

			if (orderBy != null) {
				var sortColumns = orderBy.orderByElements().orderByElement().Select(x => {
					bool asc = x.DESC() == null;
					var exp = Expression.Build(x.expression());
					return new SortColumn(exp, asc);
				});

				statement.OrderBy = sortColumns;
			}

			if (forUpdate != null) {
				// TODO: support FOR UPDATE in Select
				throw new NotImplementedException();
			}

			var limit = context.queryLimitClause();
			if (limit != null) {
				var n1 = Number.PositiveInteger(limit.n1);
				var n2 = Number.PositiveInteger(limit.n2);

				if (n1 == null)
					throw new ParseCanceledException("Invalid LIMIT clause");

				if (n2 != null) {
					statement.Limit = new QueryLimit(n1.Value, n2.Value);
				} else {
					statement.Limit = new QueryLimit(n1.Value);
				}
			}

			return statement;
		}
	}
}
