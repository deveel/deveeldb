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
using System.Threading.Tasks;

namespace Deveel.Data.Sql.Tables.Model {
	public class LimitedTable : FilterTable {
		private readonly long offset;
		private readonly long count;

		public LimitedTable(ITable table, long offset, long count)
			: base(table) {
			if (offset < 0)
				offset = 0;

			if (offset + count > table.RowCount)
				throw new ArgumentException();

			this.offset = offset;
			this.count = count;
		}

		public override long RowCount => NormalizeCount(base.RowCount);

		private long NormalizeRow(long rowNumber) {
			rowNumber += offset;
			return rowNumber;
		}

		private long NormalizeCount(long rowCount) {
			rowCount -= offset;
			return System.Math.Min(rowCount, count);
		}

		public override Task<SqlObject> GetValueAsync(long row, int column) {
			return base.GetValueAsync(NormalizeRow(row), column);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new Enumerator(this);
		}

		#region Enumerator

		class Enumerator : IEnumerator<Row> {
			private long offset = -1;
			private LimitedTable table;

			public Enumerator(LimitedTable table) {
				this.table = table;
			}

			public void Dispose() {
				table = null;
			}

			public bool MoveNext() {
				return ++offset < table.RowCount;
			}

			public void Reset() {
				offset = -1;
			}

			public Row Current {
				get { return new Row(table, offset); }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion

	}
}
