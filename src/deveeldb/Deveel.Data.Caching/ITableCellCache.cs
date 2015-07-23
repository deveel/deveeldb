using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Caching {
	public interface ITableCellCache {
		void Set(CachedCell cell);

		bool TryGetValue(RowId rowId, int columnIndex, out DataObject value);

		void Remove(RowId rowId, int columnIndex);

		void Clear();
	}
}
