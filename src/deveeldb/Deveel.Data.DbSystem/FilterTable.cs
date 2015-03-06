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
using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public class FilterTable : Table {
		private ColumnIndex[] indices;

		public FilterTable(Table parent) {
			Parent = parent;
		}

		protected Table Parent { get; private set; }

		protected override IDatabase Database {
			get { return ((IDbTable) Parent).Database; }
		}

		public override TableInfo TableInfo {
			get { return Parent.TableInfo; }
		}

		public override int RowCount {
			get { return Parent.RowCount; }
		}

		public override int ColumnCount {
			get { return Parent.ColumnCount; }
		}

		public override bool HasRootsLocked {
			get { return Parent.HasRootsLocked; }
		}

		protected override int IndexOfColumn(ObjectName columnName) {
			return Parent.FindColumn(columnName);
		}

		protected override ObjectName GetResolvedColumnName(int column) {
			return (Parent as IDbTable).GetResolvedColumnName(column);
		}

		protected override ColumnIndex GetIndex(int column, int originalColumn, ITable table) {
			if (indices == null) {
				indices = new ColumnIndex[Parent.ColumnCount];
			}

			// Is there a local scheme available?
			var index = indices[column];
			if (index == null) {
				// If we are asking for the selectable schema of this table we must
				// tell the parent we are looking for its selectable scheme.
				var t = table;
				if (table == this) {
					t = Parent;
				}

				// Scheme is not cached in this table so ask the parent.
				index = ((IDbTable)Parent).GetIndex(column, originalColumn, t);

				if (table == this) {
					indices[column] = index;
				}
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
			
			return ((IDbTable) Parent).ResolveRows(column, rowSet, ancestor);
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			return Parent.GetValue(rowNumber, columnOffset);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return Parent.GetEnumerator();
		}

		public override void LockRoot(int lockKey) {
			Parent.LockRoot(lockKey);
		}

		public override void UnlockRoot(int lockKey) {
			Parent.UnlockRoot(lockKey);
		}
	}
}
