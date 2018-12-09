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
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;

namespace Deveel.Data.Sql.Tables {
	public abstract class JoinedTable : TableBase {
		protected JoinedTable(ITable[] tables) 
			: this(tables, -1) {
		}

		protected JoinedTable(ITable[] tables, int sortColumn) 
			: this(new ObjectName("#VIRTUAL_TABLE#"), tables, sortColumn) {
		}

		protected JoinedTable(ObjectName tableName, ITable[] tables) 
			: this(tableName, tables, -1) {
		}

		protected JoinedTable(ObjectName tableName, ITable[] tables, int sortColumn) {
			var tableInfos = tables.Select(x => x.ObjectInfo).Cast<TableInfo>().ToArray();

			JoinedTableInfo = new JoinedTableInfo(tableName, tableInfos);
			Tables = tables;

			Indexes = new TableIndex[JoinedTableInfo.Columns.Count];
			SortColumn = sortColumn;
		}

		public override TableInfo TableInfo => JoinedTableInfo;

		protected JoinedTableInfo JoinedTableInfo { get; }

		protected ITable[] Tables { get; }

		protected TableIndex[] Indexes { get; }

		protected int SortColumn { get; }

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			int tableNum = JoinedTableInfo.GetTableOffset(column);
			var parentTable = Tables[tableNum];
			var resolvedRow = ResolveTableRow(row, tableNum);
			return parentTable.GetValueAsync(resolvedRow, JoinedTableInfo.GetColumnOffset(column));
		}

		protected abstract IEnumerable<long> ResolveTableRows(IEnumerable<long> rowSet, int tableNum);

		protected long ResolveTableRow(long row, int tableNum) {
			return ResolveTableRows(new[] {row}, tableNum).First();
		}

		protected override IEnumerable<long> ResolveRows(int column, IEnumerable<long> rowSet, ITable ancestor) {
			if (ancestor == this)
				return new BigArray<long>(0);

			int tableNum = JoinedTableInfo.GetTableOffset(column);
			var parentTable = Tables[tableNum];

			if (!(parentTable is IVirtualTable))
				throw new InvalidOperationException();

			// Resolve the rows into the parents indices
			var rows = ResolveTableRows(rowSet, tableNum);

			return parentTable.ResolveRows(JoinedTableInfo.GetColumnOffset(column), rows, ancestor);
		}

		

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var size = RowCount;
			var allList = new BigList<long>(size);

			for (long i = 0; i < size; ++i) {
				allList.Add(i);
			}

			return GetRawTableInfo(rootInfo, allList);
		}

		private BigList<long> CalculateTableRows() {
			var size = RowCount;
			var allList = new BigList<long>(size);
			for (int i = 0; i < size; ++i) {
				allList.Add(i);
			}
			return allList;
		}

		private RawTableInfo GetRawTableInfo(RawTableInfo info, BigList<long> rows) {
			if (this is IRootTable) {
				info.Add((IRootTable)this, CalculateTableRows());
			} else {
				for (int i = 0; i < Tables.Length; ++i) {

					// Resolve the rows into the parents indices.
					var newRowSet = ResolveTableRows(rows, i).ToBigList();

					var table = Tables[i];
					if (table is IRootTable) {
						info.Add((IRootTable)table, newRowSet);
					} else if (table is JoinedTable) {
						((JoinedTable)table).GetRawTableInfo(info, newRowSet);
					}
				}
			}

			return info;
		}

		protected override TableIndex GetColumnIndex(int column, int originalColumn, ITable ancestor) {
			// First check if the given SelectableScheme is in the column_scheme array
			var scheme = Indexes[column];
			if (scheme != null) {
				if (ancestor == this)
					return scheme;

				return scheme.Subset(ancestor, originalColumn);
			}

			// If it isn't then we need to calculate it
			TableIndex index;

			// Optimization: The table may be naturally ordered by a column.  If it
			// is we don't try to generate an ordered set.
			if (SortColumn != -1 &&
			    SortColumn == column) {
				var columnName = JoinedTableInfo.Columns[column].ColumnName;
				var indexInfo = new IndexInfo(new ObjectName(TableInfo.TableName, $"#COLIDX[{column}]"), TableInfo.TableName, columnName);
				index = new InsertSearchIndex(indexInfo, this, CalculateTableRows());

				Indexes[column] = index;
				if (ancestor != this) {
					index = index.Subset(ancestor, originalColumn);
				}

			} else {
				// Otherwise we must generate the ordered set from the information in
				// a parent index.
				var parentTable = Tables[JoinedTableInfo.GetTableOffset(column)];
				index = parentTable.GetColumnIndex(JoinedTableInfo.GetColumnOffset(column), originalColumn, ancestor);
				if (ancestor == this) {
					Indexes[column] = index;
				}
			}

			return index;
		}
	}
}