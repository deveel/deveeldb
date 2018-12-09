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
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;

namespace Deveel.Data.Sql.Tables {
	public class FilterTable : TableBase {
		private TableIndex[] columnIndices;

		public FilterTable(ITable parent) {
			Parent = parent;
		}

		protected ITable Parent { get; }

		public override TableInfo TableInfo => Parent.TableInfo;
		
		public override long RowCount => Parent.RowCount;

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			return Parent.GetValueAsync(row, column);
		}

		public override IEnumerator<Row> GetEnumerator()
			=> Parent.GetEnumerator();

		protected override IEnumerable<long> ResolveRows(int column, IEnumerable<long> rows, ITable ancestor) {
			if (ancestor == this || ancestor == Parent)
				return rows;

			return  Parent.ResolveRows(column, rows, ancestor);
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			return Parent.GetRawTableInfo(rootInfo);
		}

		protected override TableIndex GetColumnIndex(int column, int originalColumn, ITable ancestor) {
			if (columnIndices == null) {
				columnIndices = new TableIndex[Parent.TableInfo.Columns.Count];
			}

			// Is there a local index available?
			var index = columnIndices[column];
			if (index == null) {
				// If we are asking for the index of this table we must
				// tell the parent we are looking for its index.
				var t = ancestor;
				if (ancestor == this)
					t = Parent;

				// Index is not cached in this table so ask the parent.
				index = Parent.GetColumnIndex(column, originalColumn, t);

				if (ancestor == this)
					columnIndices[column] = index;

			} else {
				// If this has a cached scheme and we are in the correct domain then
				// return it.
				if (ancestor == this)
					return index;

				// Otherwise we must calculate the subset of the scheme
				return index.Subset(ancestor, originalColumn);
			}

			return index;
		}
	}
}