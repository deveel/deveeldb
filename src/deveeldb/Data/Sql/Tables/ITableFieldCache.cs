using System;

namespace Deveel.Data.Sql.Tables {
	public interface ITableFieldCache {
		void SetValue(ObjectName tableName, long row, int column, SqlObject value);

		bool TryGetValue(ObjectName tableName, long row, int column, out SqlObject value);

		void Remove(ObjectName tableName, long row, int column);

		void Clear();
	}
}