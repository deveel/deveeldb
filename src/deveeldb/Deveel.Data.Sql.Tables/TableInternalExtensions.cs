// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Index;

namespace Deveel.Data.Sql.Tables {
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

		internal static RawTableInfo GetRawTableInfo(this ITable table) {
			return GetRawTableInfo(table, new RawTableInfo());
		}

		internal static RawTableInfo GetRawTableInfo(this ITable table, RawTableInfo info) {
			if (table is IQueryTable)
				return ((IQueryTable)table).GetRawTableInfo(info);

			throw new NotSupportedException();
		}

		internal static void Lock(this ITable table) {
			if (table is IQueryTable)
				((IQueryTable)table).Lock();
		}

		internal static void Release(this ITable table) {
			if (table is IQueryTable)
				((IQueryTable)table).Release();
		}
	}
}
