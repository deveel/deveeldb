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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Tables {
	class LimitedTable : FilterTable {
		public long Offset { get; private set; }

		public long Total { get; private set; }

		public LimitedTable(ITable parent, long offset, long total) 
			: base(parent) {
			if (offset < 0)
				offset = 0;

			Offset = offset;
			Total = total;
		}

		private long NormalizeRow(long rowNumber) {
			rowNumber += Offset;
			return rowNumber;
		}

		private int NormalizeCount(long count) {
			count -= Offset;
			return (int) System.Math.Min(count, Total);
		}

		public override int RowCount {
			get { return NormalizeCount(base.RowCount); }
		}

		public override Field GetValue(long rowNumber, int columnOffset) {
			if (rowNumber >= RowCount)
				throw new ArgumentOutOfRangeException("rowNumber");

			return base.GetValue(NormalizeRow(rowNumber), columnOffset);
		}

		public override IEnumerator<Row> GetEnumerator() {
			return new Enumerator(this);
		}

		#region Enumerator

		class Enumerator : IEnumerator<Row> {
			private int offset = -1;
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
				get { return table.GetRow(offset); }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}
