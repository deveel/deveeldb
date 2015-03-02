// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql {
	public static class TableQueryExtensions {
		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex, DataObject value) {
			var columnType = table.TableInfo[columnIndex].ColumnType;
			var compareValue = value.CastTo(columnType);
			return table.GetIndex(columnIndex).SelectEqual(compareValue);
		}

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex, string value) {
			return table.SelectEqual(columnIndex, DataObject.VarChar(value));
		}

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex, SqlNumber value) {
			return table.SelectEqual(columnIndex, DataObject.Number(value));
		}

		public static IEnumerable<int> SelectEqual(this ITable table, int columnIndex1, DataObject value1, int columnIndex2, DataObject value2) {
			var columnType1 = table.TableInfo[columnIndex1].ColumnType;
			var columnType2 = table.TableInfo[columnIndex2].ColumnType;

			var compareValue1 = value1.CastTo(columnType1);
			var compareValue2 = value2.CastTo(columnType2);

			var result = new List<int>();

			var index1 = table.GetIndex(columnIndex1).SelectEqual(compareValue1);
			foreach (var rowIndex in index1) {
				var tableValue = table.GetValue(rowIndex, columnIndex2);
				if (tableValue.IsEqualTo(compareValue2))
					result.Add(rowIndex);
			}

			return result;
		}

		public static bool RemoveRow(this IMutableTable table, int rowIndex) {
			return table.RemoveRow(new RowId(table.TableInfo.Id, rowIndex));
		}
	}
}