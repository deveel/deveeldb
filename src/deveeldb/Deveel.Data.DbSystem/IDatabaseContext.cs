using System;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Query;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseContext : IDisposable {
		IDbConfig Configuration { get; }

		ISystemContext SystemContext { get; }

		IStoreSystem StoreSystem { get; }

		IQueryPlanner QueryPlanner { get; }

		IRoutineResolver RoutineResolver { get; }

		TableCellCache CellCache { get; }

		Locker Locker { get; }
	}
}
