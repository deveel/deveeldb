using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	public sealed class Query<T> : IOrderedQueryable<T> {
		private readonly QueryProvider provider;
		private readonly Expression expression;

		public Query(QueryProvider provider, Expression expression) {
			if (provider == null)
				throw new ArgumentNullException("provider");
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
				throw new ArgumentOutOfRangeException("expression");

			this.provider = provider;
			this.expression = expression;
		}

		public IEnumerator<T> GetEnumerator() {
			return ((IEnumerable<T>)provider.ExecuteExpression(expression)).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public Expression Expression {
			get { return expression; }
		}

		public Type ElementType {
			get { return typeof(T); }
		}

		public IQueryProvider Provider {
			get { return provider; }
		}
	}
}
