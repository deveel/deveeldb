using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	class CommitTable : IMutableTable {

		IDbObjectInfo IDbObject.ObjectInfo => TableInfo;

		int IComparable.CompareTo(object obj) {
			throw new NotImplementedException();
		}

		int IComparable<ISqlValue>.CompareTo(ISqlValue other) {
			throw new NotImplementedException();
		}

		bool ISqlValue.IsComparableTo(ISqlValue other) {
			throw new NotImplementedException();
		}

		ITableEventRegistry IMutableTable.EventRegistry => Registry;

		public TableEventRegistry Registry { get; }

		public ITableSource TableSource { get; }

		public int TableId => TableSource.TableId;


		public Task AddRowAsync(Row row) {
			throw new NotImplementedException();
		}

		public Task UpdateRowAsync(Row row) {
			throw new NotImplementedException();
		}

		public Task<bool> RemoveRowAsync(Row row) {
			throw new NotImplementedException();
		}

		public IEnumerator<Row> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

		public TableInfo TableInfo => TableSource.TableInfo;

		public long RowCount { get; }

		public Task<SqlObject> GetValueAsync(long row, int column) {
			throw new NotImplementedException();
		}

		public TableIndex GetIndex(int[] columns) {
			throw new NotImplementedException();
		}
	}
}