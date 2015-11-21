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

		public override DataObject GetValue(long rowNumber, int columnOffset) {
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
