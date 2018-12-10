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

namespace Deveel.Data.Sql.Tables.Model {
	public class VirtualTable : JoinedTable {
		private long rowCount;

		public VirtualTable(ITable[] tables, IEnumerable<long>[] rows)
			: this(tables, rows, -1) {
		}

		public VirtualTable(ITable[] tables, IEnumerable<long>[] rows, int sortColumn)
			: base(tables, sortColumn) {
			InitRows(rows);
		}

		public VirtualTable(ITable table, IEnumerable<long> rows)
			: this(table, rows, -1) {
		}

		public VirtualTable(ITable table, IEnumerable<long> rows, int sortColumn)
			: this(new[] {table}, new[] {rows}, sortColumn) {
		}

		public VirtualTable(ObjectName tableName, ITable[] tables, IEnumerable<long>[] rows)
			: this(tableName, tables, rows, -1) {
		}

		public VirtualTable(ObjectName tableName, ITable[] tables, IEnumerable<long>[] rows, int sortColumn)
			: base(tableName, tables, sortColumn) {
			InitRows(rows);
		}

		public VirtualTable(ObjectName tableName, ITable table, IEnumerable<long> rows)
			: this(tableName, table, rows, -1) {
		}

		public VirtualTable(ObjectName tableName, ITable table, IEnumerable<long> rows, int sortColumn)
			: this(tableName, new[] {table}, new[] {rows}, sortColumn) {
		}

		public override long RowCount => rowCount;

		protected BigArray<long>[] Rows { get; private set; }

		private void InitRows(IEnumerable<long>[] rowEnum) {
			Rows = new BigArray<long>[Tables.Length];

			for (int i = 0; i < Tables.Length; ++i) {
				Rows[i] = rowEnum[i].ToBigArray();
			}

			if (Rows.Length > 0) {
				rowCount = Rows[0].Length;
			}

		}

		protected override IEnumerable<long> ResolveTableRows(IEnumerable<long> rowSet, int tableNum) {
			var result = rowSet.ToBigArray();
			var curRowList = Rows[tableNum];

			for (long n = result.Length - 1; n >= 0; --n) {
				var row1 = result[n];
				long row2;

				if (row1 == Int64.MaxValue - 1) {
					row2 = 0;
				}
				else if (row1 < 0 || row1 >= curRowList.Length) {
					throw new InvalidOperationException("Invalid row reference");
				}
				else {
					row2 = curRowList[row1];
				}

				result[n] = row2;
			}

			return result;
		}
	}
}
