using System;
using System.Linq;

namespace Deveel.Data.Linq {
	public static class QueryExtensions {
		public static IQueryable<T> AsQueryable<T>(this IQuery query) {
			return new LinqQueryable<T>(query);
		}
	}
}
