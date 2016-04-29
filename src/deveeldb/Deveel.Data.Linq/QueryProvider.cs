using System;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	public abstract class QueryProvider : IQueryProvider {
		public IQueryable CreateQuery(Expression expression) {
			throw new NotImplementedException();
		}

		IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression) {
			return new Query<TElement>(this, expression);
		}

		protected abstract object Execute(Expression expression);

		internal object ExecuteExpression(Expression expression) {
			return Execute(expression);
		}

		object IQueryProvider.Execute(Expression expression) {
			return Execute(expression);
		}

		TResult IQueryProvider.Execute<TResult>(Expression expression) {
			return (TResult) Execute(expression);
		}
	}
}
