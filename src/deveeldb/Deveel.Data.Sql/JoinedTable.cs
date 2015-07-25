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

using Deveel.Data.DbSystem;
using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	abstract class JoinedTable : Table {
		private ITable[] referenceList;
		private ColumnIndex[] indexes;

		// These two arrays are lookup tables created in the constructor.  They allow
		// for quick resolution of where a given column should be 'routed' to in
		// the ancestors.

		private int[] columnTable;
		private int[] columnFilter;

		private TableInfo vtTableInfo;

		private byte rootsLocked;

		public JoinedTable(IEnumerable<ITable> tables) {
			SortColumn = -1;
			Init(tables);
		}

		public JoinedTable(ITable table)
			: this(new[] {table}) {
		}

		protected ColumnIndex[] ColumnIndices {
			get { return indexes; }
		}

		protected int[] ColumnTable {
			get { return columnTable; }
		}

		protected int[] ColumnFilter {
			get { return columnFilter; }
		}

		protected virtual void Init(IEnumerable<ITable> tables) {
			var tablesArray = tables.ToArray();
			referenceList = tablesArray;

			int colCount = ColumnCount;
			indexes = new ColumnIndex[colCount];

			vtTableInfo = new TableInfo(new ObjectName("#VIRTUAL TABLE#"));

			// Generate look up tables for column_table and column_filter information

			columnTable = new int[colCount];
			columnFilter = new int[colCount];

			int index = 0;
			for (int i = 0; i < referenceList.Length; ++i) {
				var curTable = referenceList[i];
				var curTableInfo = curTable.TableInfo;
				int refColCount = curTable.ColumnCount();

				// For each column
				for (int n = 0; n < refColCount; ++n) {
					columnFilter[index] = n;
					columnTable[index] = i;
					++index;

					// Add this column to the data table info of this table.
					var columnInfo = curTableInfo[n];
					var newColumnInfo = new ColumnInfo(columnInfo.ColumnName, columnInfo.ColumnType) {
						DefaultExpression = columnInfo.DefaultExpression,
						IsNotNull = columnInfo.IsNotNull,
						IndexType = columnInfo.IndexType
					};

					vtTableInfo.AddColumnSafe(newColumnInfo);
				}
			}

			vtTableInfo = vtTableInfo.AsReadOnly();
		}

		protected override int ColumnCount {
			get {
				int columnCountSum = 0;
				for (int i = 0; i < referenceList.Length; ++i) {
					columnCountSum += referenceList[i].ColumnCount();
				}
				return columnCountSum;
			}
		}

		public override IDatabaseContext DatabaseContext {
			get { return referenceList[0].DatabaseContext; }
		}

		public override TableInfo TableInfo {
			get { return vtTableInfo; }
		}

		protected ITable[] ReferenceTables {
			get { return referenceList; }
		}

		public int SortColumn { get; set; }

		public override void LockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked++;
			for (int i = 0; i < referenceList.Length; ++i) {
				referenceList[i].LockRoot(lockKey);
			}
		}

		public override void UnlockRoot(int lockKey) {
			// For each table, recurse.
			rootsLocked--;
			for (int i = 0; i < referenceList.Length; ++i) {
				referenceList[i].UnlockRoot(lockKey);
			}
		}

		private IList<int> CalculateRowReferenceList() {
			int size = RowCount;
			List<int> allList = new List<int>(size);
			for (int i = 0; i < size; ++i) {
				allList.Add(i);
			}
			return allList;
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			int colIndex = 0;
			for (int i = 0; i < referenceList.Length; ++i) {
				int col = referenceList[i].FindColumn(columnName);
				if (col != -1)
					return col + colIndex;

				colIndex += referenceList[i].ColumnCount();
			}
			return -1;
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var allList = new List<int>();
			int size = RowCount;
			for (int i = 0; i < size; ++i) {
				allList.Add(i);
			}

			return GetRawTableInfo(rootInfo, allList);
		}

		private RawTableInfo GetRawTableInfo(RawTableInfo info, IEnumerable<int> rows) {
			if (this is IRootTable) {
				info.Add((IRootTable)this, CalculateRowReferenceList());
			} else {
				for (int i = 0; i < referenceList.Length; ++i) {

					IEnumerable<int> newRowSet = new List<int>(rows);

					// Resolve the rows into the parents indices.
					newRowSet = ResolveRowsForTable(newRowSet, i);

					var table = referenceList[i];
					if (table is IRootTable) {
						info.Add((IRootTable)table, newRowSet.ToArray());
					} else if (table is JoinedTable) {
						((JoinedTable)table).GetRawTableInfo(info, newRowSet);
					}
				}
			}

			return info;
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			var parentTable = referenceList[columnTable[column]];
			return parentTable.GetResolvedColumnName(columnFilter[column]);
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			int tableNum = columnTable[columnOffset];
			var parentTable = referenceList[tableNum];
			rowNumber = ResolveRowForTable((int)rowNumber, tableNum);
			return parentTable.GetValue(rowNumber, columnFilter[columnOffset]);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			// First check if the given SelectableScheme is in the column_scheme array
			var scheme = indexes[column];
			if (scheme != null) {
				if (table == this)
					return scheme;

				return scheme.GetSubset(table, originalColumn);
			}

			// If it isn't then we need to calculate it
			ColumnIndex index;

			// Optimization: The table may be naturally ordered by a column.  If it
			// is we don't try to generate an ordered set.
			if (SortColumn != -1 &&
				SortColumn == column) {
				var isop = new InsertSearchIndex(this, column, CalculateRowReferenceList());
				isop.RecordUid = false;
				index = isop;
				indexes[column] = index;
				if (table != this) {
					index = index.GetSubset(table, originalColumn);
				}

			} else {
				// Otherwise we must generate the ordered set from the information in
				// a parent index.
				var parentTable = referenceList[columnTable[column]];
				index = parentTable.GetIndex(columnFilter[column], originalColumn, table);
				if (table == this) {
					indexes[column] = index;
				}
			}

			return index;
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (ancestor == this)
				return new int[0];

			int tableNum = columnTable[column];
			var parentTable = referenceList[tableNum];

			// Resolve the rows into the parents indices
			var rows = ResolveRowsForTable(rowSet, tableNum);

			return parentTable.ResolveRows(columnFilter[column], rows, ancestor);
		}

		protected abstract IEnumerable<int> ResolveRowsForTable(IEnumerable<int> rowSet, int tableNum);

		protected int ResolveRowForTable(int rowNumber, int tableNum) {
			return ResolveRowsForTable(new[] {rowNumber}, tableNum).First();
		}
	}
}
