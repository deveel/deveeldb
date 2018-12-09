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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Tables {
	public class JoinedTableInfo : TableInfo {
		private readonly TableInfo[] tableInfos;
		private int[] columnTable;
		private int[] columnFilter;

		public JoinedTableInfo(TableInfo[] tableInfos) 
			: this(new ObjectName("#VTABLE#"), tableInfos) {
		}

		public JoinedTableInfo(ObjectName tableName, TableInfo[] tableInfos) 
			: base(tableName) {
			this.tableInfos = tableInfos;
			Columns = new JoinedColumnList(this);
		}

		public override IColumnList Columns { get; }

		public int GetTableOffset(int columnOffset) {
			return columnTable[columnOffset];
		}

		public int GetColumnOffset(int columnOffset) {
			return columnFilter[columnOffset];
		}

		#region JoinedColumnList

		class JoinedColumnList : IColumnList {
			private readonly JoinedTableInfo tableInfo;

			private List<ColumnInfo> columns;

			public JoinedColumnList(JoinedTableInfo tableInfo) {
				this.tableInfo = tableInfo;
				columns = new List<ColumnInfo>();

				foreach (var info in tableInfo.tableInfos) {
					Count += info.Columns.Count;
				}

				tableInfo.columnTable = new int[Count];
				tableInfo.columnFilter = new int[Count];

				int index = 0;
				for (int i = 0; i < tableInfo.tableInfos.Length; ++i) {
					var curTableInfo = tableInfo.tableInfos[i];
					int refColCount = curTableInfo.Columns.Count;

					for (int n = 0; n < refColCount; ++n) {
						tableInfo.columnFilter[index] = n;
						tableInfo.columnTable[index] = i;
						++index;

						var columnInfo = curTableInfo.Columns[n];
						var newColumnInfo = new ColumnInfo(columnInfo.ColumnName, columnInfo.ColumnType) {
							DefaultValue = columnInfo.DefaultValue
						};

						columns.Add(newColumnInfo);
					}
				}
			}

			public IEnumerator<ColumnInfo> GetEnumerator() {
				return columns.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public void Add(ColumnInfo item) {
				throw new NotSupportedException();
			}

			public void Clear() {
				throw new NotSupportedException();
			}

			public bool Contains(ColumnInfo item) {
				return IndexOf(item.ColumnName) != -1;
			}

			public void CopyTo(ColumnInfo[] array, int arrayIndex) {
				columns.CopyTo(array, arrayIndex);
			}

			public bool Remove(ColumnInfo item) {
				throw new NotSupportedException();
			}

			public bool Remove(string columnName) {
				throw new NotSupportedException();
			}

			public int Count { get; }

			public bool IsReadOnly => true;

			public int IndexOf(ColumnInfo item) {
				return IndexOf(item.ColumnName);
			}

			public void Insert(int index, ColumnInfo item) {
				throw new NotSupportedException();
			}

			public void RemoveAt(int index) {
				throw new NotSupportedException();
			}

			public ColumnInfo this[int index] {
				get { return columns[index]; }
				set { throw new NotSupportedException(); }
			}

			public ColumnInfo this[string columnName] {
				get { return columns[IndexOf(columnName)]; }
			}

			public int IndexOf(ObjectName columnName) {
				int colIndex = 0;
				for (int i = 0; i < tableInfo.tableInfos.Length; ++i) {
					int col = tableInfo.tableInfos[i].Columns.IndexOf(columnName);
					if (col != -1)
						return col + colIndex;

					colIndex += tableInfo.tableInfos[i].Columns.Count;
				}
				return -1;
			}

			public int IndexOf(string columnName) {
				int colIndex = 0;
				for (int i = 0; i < tableInfo.tableInfos.Length; ++i) {
					int col = tableInfo.tableInfos[i].Columns.IndexOf(columnName);
					if (col != -1)
						return col + colIndex;

					colIndex += tableInfo.tableInfos[i].Columns.Count;
				}
				return -1;
			}

			public ObjectName GetColumnName(int offset) {
				var parentTable = tableInfo.tableInfos[tableInfo.columnTable[offset]];
				return parentTable.Columns.GetColumnName(tableInfo.columnFilter[offset]);

			}
		}

		#endregion
	}
}