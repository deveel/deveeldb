using System;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	public static class TableExtensions {
		public static IQueryable AsQueryable(this ITable table, Type elementType) {
			return new TableQueryable(new TableQueryProvider(table), elementType);
		}

		public static IQueryable<T> AsQueryable<T>(this ITable table) {
			throw new NotImplementedException();
		} 
	}
}
