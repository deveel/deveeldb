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

using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class Row {
		private Dictionary<int, DataObject> values;

		public Row(ITable table, RowId rowId) {
			if (table == null)
				throw new ArgumentNullException("table");

			Table = table;
			RowId = rowId;
		}

		public ITable Table { get; private set; }

		public RowId RowId { get; private set; }

		public DataObject this[int columnOffset] {
			get { return GetValue(columnOffset); }
			set { SetValue(columnOffset, value); }
		}

		public DataObject GetValue(int columnOffset) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (RowId.IsNull)
				throw new InvalidOperationException("Row was not established to any table.");

			if (values == null) {
				var colCount = Table.TableInfo.ColumnCount;
				values = new Dictionary<int, DataObject>(colCount);

				for (int i = 0; i < colCount; i++) {
					values[i] = Table.GetValue(RowId, i);
				}
			}

			DataObject value;
			if (!values.TryGetValue(columnOffset, out value))
				// TODO: Set the return NULL from the type of the column...
				return DataObject.Null();

			return value;
		}

		public void SetValue(int columnOffset, DataObject value) {
			if (columnOffset < 0 || columnOffset >= Table.TableInfo.ColumnCount)
				throw new ArgumentOutOfRangeException("columnOffset");

			if (RowId.IsNull)
				throw new InvalidOperationException("Row was not established to any table.");

			if (values == null) {
				var colCount = Table.TableInfo.ColumnCount;
				values = new Dictionary<int, DataObject>(colCount);
			}

			values[columnOffset] = value;
		}
	}
}