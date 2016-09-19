using System;
using System.Linq;
using System.Linq.Expressions;

using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;

namespace Deveel.Data.Linq {
	class LinqQueryable<T> : QueryableBase<T> {
		public LinqQueryable(IQueryProvider provider, Expression expression) 
			: base(provider, expression) {
		}

		public LinqQueryable(IQuery query)
			: base(QueryParser.CreateDefault(), new LinqQueryExecutor(query)) {
		}
	}
}
