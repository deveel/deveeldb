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

namespace Deveel.Data.Sql.Expressions {
	class PreparerVisitor : SqlExpressionVisitor {
		private readonly IExpressionPreparer preparer;

		public PreparerVisitor(IExpressionPreparer preparer) {
			this.preparer = preparer;
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (preparer != null && 
				preparer.CanPrepare(expression))
				expression = preparer.Prepare(expression);

			return base.Visit(expression);
		}

		public override SqlExpression VisitAssign(SqlAssignExpression assign) {
			var reference = assign.ReferenceExpression;
			if (reference != null)
				reference = reference.Prepare(preparer);

			var valueExpression = assign.ValueExpression;
			if (valueExpression != null)
				valueExpression = valueExpression.Prepare(preparer);

			return SqlExpression.Assign(reference, valueExpression);
		}

		private static IEnumerable<T> Prepare<T>(IEnumerable<T> list, IExpressionPreparer preparer)
			where T : IPreparable {
			var newList = new List<T>();
			foreach (var item in list) {
				var newItem = item;
				if (newItem != null) {
					newItem = (T)newItem.Prepare(preparer);
				}

				newList.Add(newItem);
			}

			return newList.ToArray();
		}

		public override SqlExpression VisitQuery(SqlQueryExpression query) {
			var selectColumns = Prepare(query.SelectColumns, preparer);
			var newExpression = new SqlQueryExpression(selectColumns);

			var where = query.WhereExpression;
			if (where != null)
				where = where.Prepare(preparer);

			newExpression.WhereExpression = where;

			var having = query.HavingExpression;
			if (having != null)
				having = having.Prepare(preparer);

			newExpression.HavingExpression = having;

			var from = query.FromClause;
			if (from != null)
				from = (FromClause) ((IPreparable)from).Prepare(preparer);

			query.FromClause = from;

			var nextComposite = query.NextComposite;
			if (nextComposite != null)
				nextComposite = (SqlQueryExpression) nextComposite.Prepare(preparer);

			query.NextComposite = nextComposite;

			return newExpression;
		}
	}
}