// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Tables.Model;

namespace Deveel.Data.Sql.Tables {
	public static class TableExtensions {
		#region Internal Helpers

		internal static RawTableInfo GetRawTableInfo(this ITable table, RawTableInfo rootInfo) {
			if (table is IVirtualTable)
				return ((IVirtualTable) table).GetRawTableInfo(rootInfo);
			if (table is IRootTable)
				return ((IRootTable) table).GetRawTableInfo(rootInfo);

			throw new NotSupportedException();
		}

		internal static RawTableInfo GetRawTableInfo(this ITable table)
			=> table.GetRawTableInfo(new RawTableInfo());

		internal static RawTableInfo GetRawTableInfo(this IRootTable table, RawTableInfo rootInfo) {
			var rows = table.Select(x => x.Number).ToBigList();
			rootInfo.Add(table, rows);

			return rootInfo;
		}

		internal static IEnumerable<long> ResolveRows(this ITable table, int column, IEnumerable<long> rows,
			ITable ancestor) {
			if (table is IVirtualTable)
				return ((IVirtualTable) table).ResolveRows(column, rows, ancestor);

			if (table != ancestor)
				throw new InvalidOperationException();

			return rows;
		}

		internal static TableIndex GetColumnIndex(this ITable table, int column, int originalColumn, ITable ancestor) {
			if (table is IVirtualTable)
				return ((IVirtualTable) table).GetColumnIndex(column, originalColumn, ancestor);

			throw new NotSupportedException();
		}


		#endregion

		#region Rows

		public static Row NewRow(this IMutableTable table) {
			return new Row(table);
		}

		public static Row GetRow(this ITable table, long row)
			=> new Row(table, row);

		public static IEnumerable<long> SelectAllRows(this ITable table, int column) {
			return table.GetIndex(new[] {column}).SelectAll();
		}

		#endregion

		#region Values

		public static SqlObject GetValue(this ITable table, long row, int column)
			=> table.GetValueAsync(row, column).Result;

		#endregion

		#region Alias

		public static ITable As(this ITable table, ObjectName alias) {
			return new AliasedTable(table, alias);
		}

		#endregion

		#region Order By

		public static IEnumerable<long> OrderRowsByColumns(this ITable table, int[] columns) {
			var work = table.OrderBy(columns);
			// 'work' is now sorted by the columns,
			// Get the rows in this tables domain,
			var rowList = work.Select(row => row.Number);

			return work.ResolveRows(0, rowList, table);
		}

		public static ITable OrderBy(this ITable table, int[] columns) {
			// Sort by the column list.
			ITable resultTable = table;
			for (int i = columns.Length - 1; i >= 0; --i) {
				resultTable = resultTable.OrderBy(columns[i], true);
			}

			// A nice post condition to check on.
			if (resultTable.RowCount != table.RowCount)
				throw new InvalidOperationException("The final row count mismatches.");

			return resultTable;
		}

		public static ITable OrderBy(this ITable table, int columnIndex, bool ascending) {
			if (table == null)
				return null;

			var rows = table.SelectAllRows(columnIndex);

			// Reverse the list if we are not ascending
			if (@ascending == false)
				rows = rows.Reverse();

			return new VirtualTable(new[] {table}, new IEnumerable<long>[] {rows});
		}

		#endregion

		#region Outer

		public static ITable Outer(this ITable table, ITable outside) {
			var tableInfo = table.GetRawTableInfo(new RawTableInfo());
			var baseTables = tableInfo.Tables;
			var baseRows = tableInfo.Rows;
			return new OuterTable(baseTables, baseRows, outside);
		}

		#endregion
	}
}