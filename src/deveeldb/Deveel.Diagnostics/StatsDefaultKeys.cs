using System;

namespace Deveel.Data.Deveel.Diagnostics {
	public static class StatsDefaultKeys {
		public const string SessionPrefix = "{Session}";

		public const string TransactionsCount = "Transactions.Count";
		public const string OpenTransactionsCount = "OpenTransactions.Count";

		public const string DataCellCacheTotalWipes = "DataCellCache.TotalWipes";
		public const string DataCellCacheClean = "DataCellCache.Clean";
		public const string DataCellCacheCurrentSize = "DataCellCache.CurrentSize";
	}
}
