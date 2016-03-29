// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Caching {
	public static class TableCacheExtensions {
		public static bool TryGetValue(this ITableCellCache cache, string database, int tableId, int rowNumber, int columnOffset, out Field value) {
			var rowId = new RowId(tableId, rowNumber);
			var key = new CellKey(database, new CellId(rowId, columnOffset));
			return cache.TryGetValue(key, out value);
		}

		public static void Set(this ITableCellCache cache, string database, int tableId, int rowNumber, int columnOffset, Field value) {
			var rowId = new RowId(tableId, rowNumber);
			var key = new CellKey(database, new CellId(rowId, columnOffset));
			cache.Set(new CachedCell(key, value));
		}

		public static void Remove(this ITableCellCache cache, string database, int tableId, int rowNumber, int columnOffset) {
			var rowId = new RowId(tableId, rowNumber);
			var key = new CellKey(database, new CellId(rowId, columnOffset));
			cache.Remove(key);
		}
	}
}
