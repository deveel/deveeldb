using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	public static class TableInternalExtensions {
		internal static int FindColumn(this ITable table, ObjectName columnName) {
			if (table is IQueryTable)
				return ((IQueryTable)table).FindColumn(columnName);

			var parent = columnName.Parent;
			if (!parent.Equals(table.TableInfo.TableName))
				return -1;

			return table.TableInfo.IndexOfColumn(columnName.Name);
		}

		internal static int ColumnCount(this ITable table) {
			if (table is IQueryTable)
				return ((IQueryTable)table).ColumnCount;

			return table.TableInfo.ColumnCount;
		}

		internal static ObjectName GetResolvedColumnName(this ITable table, int columnOffset) {
			if (table is IQueryTable)
				return ((IQueryTable)table).GetResolvedColumnName(columnOffset);

			var tableName = table.TableInfo.TableName;
			var columnName = table.TableInfo[columnOffset].ColumnName;
			return new ObjectName(tableName, columnName);
		}

		internal static IEnumerable<int> ResolveRows(this ITable table, int columnOffset, IEnumerable<int> rows,
			ITable ancestor) {
			if (table is IQueryTable)
				return ((IQueryTable)table).ResolveRows(columnOffset, rows, ancestor);

			if (table != ancestor)
				throw new ArgumentException();

			return rows.ToList();
		}

		internal static ColumnIndex GetIndex(this ITable thisTable, int column, int originalColumn, ITable table) {
			if (thisTable is IQueryTable)
				return ((IQueryTable)thisTable).GetIndex(column, originalColumn, table);

			var index = thisTable.GetIndex(column);
			if (table == thisTable)
				return index;

			// Otherwise, get the scheme to calculate a subset of the given scheme.
			return index.GetSubset(table, originalColumn);
		}

		internal static ITableVariableResolver GetVariableResolver(this ITable table) {
			if (table is IQueryTable)
				return ((IQueryTable)table).GetVariableResolver();

			// TODO: implement a default table resolver
			throw new NotImplementedException();
		}

		internal static ObjectName ResolveColumnName(this ITable table, string columnName) {
			return new ObjectName(table.TableInfo.TableName, columnName);
		}

		internal static void LockRoot(this ITable table, int lockKey) {
			if (table is IQueryTable)
				((IQueryTable)table).LockRoot(lockKey);
		}

		internal static void UnlockRoot(this ITable table, int lockKey) {
			if (table is IQueryTable)
				((IQueryTable)table).UnlockRoot(lockKey);
		}

		internal static RawTableInfo GetRawTableInfo(this ITable table) {
			return GetRawTableInfo(table, new RawTableInfo());
		}

		internal static RawTableInfo GetRawTableInfo(this ITable table, RawTableInfo info) {
			if (table is IQueryTable)
				return ((IQueryTable)table).GetRawTableInfo(info);

			throw new NotSupportedException();
		}
	}
}
