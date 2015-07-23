using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Caching {
	public static class TableCacheExtensions {
		public static bool TryGetValue(this ITableCellCache cache, int tableId, int rowNumber, int columnOffset,
			out DataObject value) {
			var rowId = new RowId(tableId, rowNumber);
			return cache.TryGetValue(rowId, columnOffset, out value);
		}

		public static void Set(this ITableCellCache cache, int tableId, int rowNumber, int columnOffset, DataObject value) {
			var rowId = new RowId(tableId, rowNumber);
			cache.Set(new CachedCell(rowId, columnOffset, value));
		}
	}
}
