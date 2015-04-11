using System;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	public static class TableExtensions {
		public static IQueryable AsQueryable(this ITable table, Type elementType) {
			var type = typeof (QueryableTable<>).MakeGenericType(elementType);
			var provider = new TableQueryProvider(table);
			return Activator.CreateInstance(type, provider) as IQueryable;
		}

		public static IQueryable<T> AsQueryable<T>(this ITable table) {
			return new QueryableTable<T>(new TableQueryProvider(table));
		} 
	}
}
