using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	class QueryableTable<T> : IOrderedQueryable<T> {
		public QueryableTable(TableQueryProvider provider) {
			Provider = provider;
			Expression = Expression.Constant(this);
		}

		public QueryableTable(TableQueryProvider provider, Expression expression) {
			if (provider == null)
				throw new ArgumentNullException("provider");
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
				throw new ArgumentException("Invalid expression type.", "expression");

			Provider = provider;
			Expression = expression;
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return ((IEnumerable) Provider.Execute(Expression)).GetEnumerator();
		}

		public Expression Expression { get; private set; }

		public Type ElementType {
			get { return typeof (T); }
		}

		public IQueryProvider Provider { get; private set; }

		public IEnumerator<T> GetEnumerator() {
			return ((IEnumerable<T>) Provider.Execute<T>(Expression)).GetEnumerator();
		}

		public override string ToString() {
			return ((TableQueryProvider) Provider).GetQueryText(Expression);
		}
	}
}
