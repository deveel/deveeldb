using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	public class TestTable : Table, IMutableTable {
		private List<Row> rows;
		private readonly TableInfo tableInfo;
		private int rowId = -1;

		public TestTable(TableInfo tableInfo) {
			this.tableInfo = tableInfo;
			rows = new List<Row>();
		}
 
		public override IEnumerator<Row> GetEnumerator() {
			return rows.GetEnumerator();
		}

		public override IDatabaseContext DatabaseContext {
			get { return null; }
		}

		public override TableInfo TableInfo {
			get { return tableInfo; }
		}

		public override int RowCount {
			get { return rows.Count; }
		}

		public override void LockRoot(int lockKey) {
		}

		public override void UnlockRoot(int lockKey) {
		}

		protected override IEnumerable<int> ResolveRows(int column, IEnumerable<int> rowSet, ITable ancestor) {
			return rowSet;
		}

		protected override RawTableInfo GetRawTableInfo(RawTableInfo rootInfo) {
			return new RawTableInfo();
		}

		public override DataObject GetValue(long rowNumber, int columnOffset) {
			var row = rows[(int) rowNumber];
			return row.GetValue(columnOffset);
		}

		protected override void Dispose(bool disposing) {
			rows = null;
			base.Dispose(disposing);
		}

		public TableEventRegistry EventRegistry { get; private set; }

		public void AddLock() {
		}

		public void RemoveLock() {
		}

		public void AddRow(Row row) {
			row.SetRowNumber(++rowId);
			rows.Add(row);
		}

		public void UpdateRow(Row row) {
			var rowNum = row.RowId.RowNumber;
			rows[rowNum] = row;
		}

		public bool RemoveRow(RowId rowId) {
			var rowNum = rowId.RowNumber;
			rows.RemoveAt(rowNum);
			return true;
		}

		public void FlushIndexes() {
		}

		public void AssertConstraints() {
		}
	}
}
