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

using Deveel.Data.DbSystem;
using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	class FilterTable : Table {
		private ColumnIndex[] columnIndices;

		public FilterTable(ITable parent) {
			Parent = parent;
		}

		protected ITable Parent { get; private set; }

		public override IEnumerator<Row> GetEnumerator() {
			return Parent.GetEnumerator();
		}

		public override IDatabaseContext DatabaseContext {
			get { return Parent.DatabaseContext; }
		}

		public override TableInfo TableInfo {
			get { return Parent.TableInfo; }
		}

		public override int RowCount {
			get { return Parent.RowCount; }
		}

		public override void LockRoot(int lockKey) {
			Parent.LockRoot(lockKey);
		}

		public override void UnlockRoot(int lockKey) {
			Parent.UnlockRoot(lockKey);
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			return Parent.GetRawTableInfo(rootInfo);
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			if (columnIndices == null) {
				columnIndices = new ColumnIndex[Parent.ColumnCount()];
			}

			// Is there a local index available?
			var index = columnIndices[column];
			if (index == null) {
				// If we are asking for the index of this table we must
				// tell the parent we are looking for its index.
				var t = table;
				if (table == this)
					t = Parent;

				// Index is not cached in this table so ask the parent.
				index = Parent.GetIndex(column, originalColumn, t);
				if (table == this)
					columnIndices[column] = index;

			} else {
				// If this has a cached scheme and we are in the correct domain then
				// return it.
				if (table == this)
					return index;

				// Otherwise we must calculate the subset of the scheme
				return index.GetSubset(table, originalColumn);
			}

			return index;
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			if (ancestor == this || ancestor == Parent)
				return rowSet;

			return Parent.ResolveRows(column, rowSet, ancestor);
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			return Parent.GetValue(rowNumber, columnOffset);
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return Parent.GetResolvedColumnName(column);
		}
	}
}
