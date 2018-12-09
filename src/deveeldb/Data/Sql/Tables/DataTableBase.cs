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

namespace Deveel.Data.Sql.Tables {
	public abstract class DataTableBase : TableBase, IRootTable {
		private TableIndex[] indexes;

		bool IEquatable<ITable>.Equals(ITable table) {
			return this == table;
		}

		protected override IEnumerable<long> ResolveRows(int column, IEnumerable<long> rows, ITable ancestor) {
			if (this != ancestor)
				throw new Exception("Method routed to incorrect table ancestor.");

			return rows;
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var rows = this.Select(row => row.Number).ToBigList();
			rootInfo.Add(this, rows);
			return rootInfo;
		}

		protected override TableIndex GetColumnIndex(int column, int originalColumn, ITable ancestor) {
			var index = GetColumnIndex(column);
			if (this == ancestor)
				return index;

			return index.Subset(ancestor, originalColumn);
		}

		public override TableIndex GetColumnIndex(int column) {
			if (indexes == null)
				throw new InvalidOperationException("The indexes for the table were not built");

			return indexes[column];
		}

		protected virtual void SetupIndexes(Type indexType) {
			indexes = new TableIndex[TableInfo.Columns.Count];
			for (int i = 0; i < TableInfo.Columns.Count; i++) {
				var columnName = TableInfo.Columns[i].ColumnName;
				var indexInfo = new IndexInfo($"#COLIDX_{i}", TableInfo.TableName, columnName);

				if (indexType == typeof(BlindSearchIndex)) {
					indexes[i] = new BlindSearchIndex(indexInfo, this);
				} else if (indexType == typeof(InsertSearchIndex)) {
					indexes[i] = new InsertSearchIndex(indexInfo, this);
				} else {
					var index = Activator.CreateInstance(indexType, indexInfo, this) as TableIndex;
					if (index == null)
						throw new InvalidOperationException();

					indexes[i] = index;
				}
			}
		}

		protected void AddRowToIndex(int rowNumber) {
			int colCount = TableInfo.Columns.Count;
			var tableInfo = TableInfo;
			for (int i = 0; i < colCount; ++i) {
				if (tableInfo.Columns[i].ColumnType.IsIndexable) {
					var index = GetColumnIndex(i);
					index.Insert(rowNumber);
				}
			}
		}

		protected void RemoveRowFromIndex(long rowNumber) {
			int colCount = TableInfo.Columns.Count;
			var tableInfo = TableInfo;
			for (int i = 0; i < colCount; ++i) {
				if (tableInfo.Columns[i].ColumnType.IsIndexable) {
					var index = GetColumnIndex(i);
					index.Remove((int)rowNumber);
				}
			}
		}
	}
}