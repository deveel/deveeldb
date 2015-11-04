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

using Deveel.Data;
using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	class CompositeTable : Table, IRootTable {
		private readonly ITable mainTable;
		private readonly ITable[] composites;

		private readonly IList<int>[] rowIndexes;
		private readonly ColumnIndex[] columnIndexes;

		private int rootsLocked;

		public CompositeTable(ITable mainTable, ITable[] composites, CompositeFunction function, bool all) {
			this.mainTable = mainTable;
			this.composites = composites;

			columnIndexes = new ColumnIndex[mainTable.TableInfo.ColumnCount];
			int size = composites.Length;
			rowIndexes = new IList<int>[size];

			if (function == CompositeFunction.Union) {
				// Include all row sets in all tables
				for (int i = 0; i < size; ++i) {
					rowIndexes[i] = composites[i].SelectAllRows().ToList();
				}

				RemoveDuplicates(all);
			} else {
				throw new InvalidOperationException("Unrecognised composite function");
			}

		}

		private void RemoveDuplicates(bool all) {
			if (!all)
				throw new NotImplementedException();
		}

		public CompositeTable(ITable[] composites, CompositeFunction function, bool all)
			: this(composites[0], composites, function, all) {
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new SimpleRowEnumerator(this);
		}

		public override IDatabaseContext DatabaseContext {
			get { return mainTable.DatabaseContext; }
		}

		protected override int ColumnCount {
			get { return mainTable.TableInfo.ColumnCount; }
		}

		public override void Lock() {
			// For each table, recurse.
			rootsLocked++;
			for (int i = 0; i < composites.Length; ++i) {
				composites[i].Lock();
			}
		}

		public override void Release() {
			// For each table, recurse.
			rootsLocked--;
			for (int i = 0; i < composites.Length; ++i) {
				composites[i].Release();
			}
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			var rows = this.Select(x => x.RowId.RowNumber).ToArray();
			rootInfo.Add(this, rows);
			return rootInfo;
		}

		public override int RowCount {
			get { return rowIndexes.Sum(t => t.Count); }
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			var index = columnIndexes[column];
			if (index == null) {
				index = new BlindSearchIndex(this, column);
				columnIndexes[column] = index;
			}

			// If we are getting a scheme for this table, simple return the information
			// from the column_trees Vector.
			if (table == this)
				return index;

			// Otherwise, get the scheme to calculate a subset of the given scheme.
			return index.GetSubset(table, originalColumn);
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (ancestor != this)
				throw new InvalidOperationException();

			return rowSet;
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			for (int i = 0; i < rowIndexes.Length; ++i) {
				var list = rowIndexes[i];
				int sz = list.Count;
				if (rowNumber < sz)
					return composites[i].GetValue(list[(int)rowNumber], columnOffset);

				rowNumber -= sz;
			}

			throw new ArgumentOutOfRangeException("rowNumber", String.Format("Row '{0}' out of range.", rowNumber));
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return mainTable.GetResolvedColumnName(column);
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			return mainTable.IndexOfColumn(columnName);
		}

		public override TableInfo TableInfo {
			get { return mainTable.TableInfo; }
		}

		public bool TypeEquals(IRootTable other) {
			return this == other;
		}
	}
}
