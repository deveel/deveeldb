using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	class TableQueryable : IQueryable {
		public TableQueryable(TableQueryProvider provider, Type elementType) 
			: this(provider, elementType, null) {
		}

		public TableQueryable(TableQueryProvider provider, Type elementType, Expression expression) {
			Provider = provider;
			ElementType = elementType;

			if (expression == null)
				expression = Expression.Constant(this);

			Expression = expression;
		}

		public IEnumerator GetEnumerator() {
			return ((IEnumerable) Provider.Execute(Expression)).GetEnumerator();
		}

		public Expression Expression { get; private set; }

		public Type ElementType { get; private set; }

		public IQueryProvider Provider { get; private set; }

		public override string ToString() {
			return ((TableQueryProvider) Provider).GetQueryText(Expression);
		}
	}
}
