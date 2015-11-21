using System;

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	public interface ITableSource {
		void SetUniqueId(long value);

		long GetNextUniqueId();

		IIndexSet CreateIndexSet();

		int TableId { get; }

		TableInfo TableInfo { get; }

		bool CanCompact { get; }

		IMutableTable CreateTableAtCommit(ITransaction transaction);

		int AddRow(Row row);

		RecordState WriteRecordState(int rowNumber, RecordState recordState);

		void BuildIndexes();
	}
}
