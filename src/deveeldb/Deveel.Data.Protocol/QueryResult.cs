using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	public sealed class QueryResult : IDisposable {
		internal QueryResult(SqlQuery query, ITable result) {
			Query = query;
			Result = result;
		}

		public SqlQuery Query { get; set; }

		public ITable Result { get; set; }

		public int RowCount { get; private set; }

		public int ColumnCount { get; private set; }

		public QueryResultColumn GetColumn(int columnOffset) {
			throw new NotImplementedException();
		}

		public void LockRoot(int lockKey) {
			Result.LockRoot(lockKey);
		}

		public void Dispose() {
			
		}

		public DataObject GetValue(int rowIndex, int columnIndex) {
			throw new NotImplementedException();
		}
	}
}
