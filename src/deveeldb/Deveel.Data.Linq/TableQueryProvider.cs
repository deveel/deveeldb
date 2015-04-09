using System;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	class TableQueryProvider : IQueryProvider {
		public TableQueryProvider(ITable table) {
			if (table == null)
				throw new ArgumentNullException("table");

			Table = table;
		}

		public ITable Table { get; private set; }

		public IQueryable CreateQuery(Expression expression) {
			throw new NotImplementedException();
		}

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression) {
			throw new NotImplementedException();
		}

		public object Execute(Expression expression) {
			throw new NotImplementedException();
		}

		public TResult Execute<TResult>(Expression expression) {
			return (TResult) Execute(expression);
		}

		public string GetQueryText(Expression expression) {
			throw new NotImplementedException();
		}
	}
}
