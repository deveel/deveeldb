using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Expressions {
	class PreparerVisitor : SqlExpressionVisitor {
		private readonly IExpressionPreparer preparer;

		public PreparerVisitor(IExpressionPreparer preparer) {
			this.preparer = preparer;
		}

		public override SqlExpression Visit(SqlExpression expression) {
			if (preparer.CanPrepare(expression))
				expression = preparer.Prepare(expression);

			return base.Visit(expression);
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

			return newList.AsReadOnly();
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