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

using Deveel.Collections;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Indexes {
	public abstract class TableIndex : IIndex {
		private static readonly SortedCollection<SqlObject, long> EmptyList;
		private static readonly SortedCollection<SqlObject, long> OneList;

		private const int OptimalSize = 250000;

		protected TableIndex(IndexInfo indexInfo, ITable table) {
			IndexInfo = indexInfo ?? throw new ArgumentNullException(nameof(indexInfo));
			Table = table ?? throw new ArgumentNullException(nameof(table));

			Columns = indexInfo.ColumnNames.Select(x => table.TableInfo.Columns.IndexOf(x)).ToArray();
		}

		static TableIndex() {
			EmptyList = new SortedCollection<SqlObject, long>();
			EmptyList.IsReadOnly = true;
			OneList = new SortedCollection<SqlObject, long>();
			OneList.Add(0);
			OneList.IsReadOnly = true;
		}

		IDbObjectInfo IDbObject.ObjectInfo => IndexInfo;

		public IndexInfo IndexInfo { get; }

		public ITable Table { get; }

		protected int[] Columns { get; }

		public virtual bool IsReadOnly => false;

		protected IndexKey GetKey(long row) {
			if (row > Table.RowCount)
				return this.NullKey();

			var values = new SqlObject[Columns.Length];
			for (int i = 0; i < Columns.Length; i++) {
				var column = Table.TableInfo.Columns[i];
				var value = Table.GetValueAsync(row, Columns[i]).Result;

				values[i] = value == null ? SqlObject.NullOf(column.ColumnType) : value;
			}

			return new IndexKey(values);
		}

		protected IEnumerable<long> OrderRows(IEnumerable<long> rows) {
			var rowSet = rows.ToBigArray();

			// The length of the set to order
			var rowSetLength = rowSet.Length;

			// Trivial cases where sorting is not required:
			// NOTE: We use readOnly objects to save some memory.
			if (rowSetLength == 0)
				return EmptyList;
			if (rowSetLength == 1)
				return OneList;

			// This will be 'row set' sorted by its entry lookup.  This must only
			// contain indices to rowSet entries.
			var newSet = new SortedCollection<IndexKey, long>();

			if (rowSetLength <= OptimalSize) {
				// If the subset is less than or equal to 250,000 elements, we generate
				// an array in memory that contains all values in the set and we sort
				// it.  This requires use of memory from the heap but is faster than
				// the no heap use method.
				var subsetList = new BigList<IndexKey>(rowSetLength);
				foreach (var row in rowSet) {
					subsetList.Add(GetKey(row));
				}

				// The comparator we use to sort
				var comparer = new SubsetIndexComparer(subsetList.ToBigArray());

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var cell = subsetList[i];
					newSet.InsertSort(cell, i, comparer);
				}

			} else {
				// This is the no additional heap use method to sorting the sub-set.

				// The comparator we use to sort
				var comparer = new IndexComparer(this, rowSet);

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var key = GetKey(rowSet[i]);
					newSet.InsertSort(key, i, comparer);
				}
			}

			return newSet;
		}

		public TableIndex Subset(ITable table, int column)
			=> Subset(table, new[] {column});

		public TableIndex Subset(ITable table, int[] columns) {
			if (table == null)
				throw new ArgumentNullException(nameof(table));
			if (columns.Length > 1)
				throw new NotSupportedException("multi-columns subset not implemented yet");

			// Resolve table rows in this table scheme domain.
			var rowSet = new BigList<long>(table.RowCount);
			foreach (var row in table) {
				rowSet.Add(row.Number);
			}

			var rows = table.ResolveRows(columns[0], rowSet, Table);

			// Generates an IIndex which contains indices into 'rowSet' in
			// sorted order.
			var newSet = OrderRows(rows).ToBigArray();

			// Our 'new_set' should be the same size as 'rowSet'
			if (newSet.Length != rowSet.Count) {
				throw new Exception("Internal sort error in finding sub-set.");
			}

			return CreateSubset(table, columns[0], newSet);
		}

		protected virtual TableIndex CreateSubset(ITable table, int column, IEnumerable<long> rows) {
			var columnName = table.TableInfo.Columns.GetColumnName(column);
			var indexInfo = new IndexInfo($"#SUBIDX_{column}", table.TableInfo.TableName, new[] {columnName.Name});
			return new InsertSearchIndex(indexInfo, table, rows);
		}

		public void Dispose() {
			
		}

		public abstract IEnumerable<long> SelectRange(IndexRange[] ranges);

		public abstract void Insert(long row);

		public abstract void Remove(long row);

		#region IndexComparer

		private class IndexComparer : ISortComparer<IndexKey, long> {
			private readonly TableIndex index;
			private readonly BigArray<long> rowSet;

			public IndexComparer(TableIndex index, BigArray<long> rowSet) {
				this.index = index;
				this.rowSet = rowSet;
			}

			public int Compare(long indexed, IndexKey val) {
				var key = index.GetKey(rowSet[indexed]);
				return key.CompareTo(val);
			}
		}

		#endregion

		#region SubsetIndexComparer

		private class SubsetIndexComparer : ISortComparer<IndexKey, long> {
			private readonly BigArray<IndexKey> subsetList;

			public SubsetIndexComparer(BigArray<IndexKey> subsetList) {
				this.subsetList = subsetList;
			}

			public int Compare(long index, IndexKey val) {
				return subsetList[index].CompareTo(val);
			}
		}

		#endregion
	}
}