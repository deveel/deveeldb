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

using Deveel.Data.Index;

namespace Deveel.Data.Sql.Tables {
	internal class OuterTable : VirtualTable {
		private readonly IList<int>[] outerRows;
		private int outerRowCount;

		private OuterTable(IEnumerable<ITable> tables, IList<IList<int>> rows)
			: base(tables, rows) {
			outerRows = new IList<int>[rows.Count];
		}

		public static OuterTable Create(ITable inputTable) {
			var baseTable = inputTable.GetRawTableInfo();
			var tables = baseTable.GetTables();
			var rows = baseTable.GetRows();

			return new OuterTable(tables, rows);
		}

		public override int RowCount {
			get { return base.RowCount + outerRowCount; }
		}

		public void MergeIn(ITable outsideTable) {
			var rawTableInfo = outsideTable.GetRawTableInfo();

			// Get the base information,
			var baseTables = ReferenceTables;
			IList<int>[] baseRows = ReferenceRows;

			// The tables and rows being merged in.
			var tables = rawTableInfo.GetTables();
			var rows = rawTableInfo.GetRows();
			// The number of rows being merged in.
			outerRowCount = rows[0].Count;

			for (int i = 0; i < baseTables.Length; ++i) {
				var btable = baseTables[i];
				int index = -1;
				for (int n = 0; n < tables.Length && index == -1; ++n) {
					if (btable == tables[n]) {
						index = n;
					}
				}

				// If the table wasn't found, then set 'NULL' to this base_table
				if (index == -1) {
					outerRows[i] = null;
				} else {
					List<int> list = new List<int>(outerRowCount);
					outerRows[i] = list;
					// Merge in the rows from the input table,
					IList<int> toMerge = rows[index];
					if (toMerge.Count != outerRowCount)
						throw new InvalidOperationException("Wrong size for rows being merged in.");

					list.AddRange(toMerge);
				}
			}
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			if (ColumnIndices[column] == null) {
				// EFFICIENCY: We implement this with a blind search...
				var index = new BlindSearchIndex(this, column);
				ColumnIndices[column] = index.GetSubset(this, column);
			}

			if (table == this)
				return ColumnIndices[column];

			return ColumnIndices[column].GetSubset(table, originalColumn);
		}

		public override Field GetValue(long rowNumber, int columnOffset) {
			int tableNum = ColumnTable[columnOffset];
			var parentTable = ReferenceTables[tableNum];

			if (rowNumber >= outerRowCount) {
				rowNumber = ReferenceRows[tableNum][(int)rowNumber - outerRowCount];
				return parentTable.GetValue(rowNumber, ColumnFilter[columnOffset]);
			}

			if (outerRows[tableNum] == null)
				// Special case, handling outer entries (NULL)
				return new Field(TableInfo[columnOffset].ColumnType, null);

			rowNumber = outerRows[tableNum][(int)rowNumber];
			return parentTable.GetValue(rowNumber, ColumnFilter[columnOffset]);
		}
	}
}