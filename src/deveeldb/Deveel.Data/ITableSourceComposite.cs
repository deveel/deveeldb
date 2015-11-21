using System;

using Deveel.Data.Index;
using Deveel.Data.Sql;

namespace Deveel.Data {
	public interface ITableSourceComposite : IDisposable {
		ITableSource CreateTableSource(TableInfo tableInfo, bool temporary);

		ITableSource CopySourceTable(ITableSource tableSource, IIndexSet indexSet);
	}
}
