using System;

using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public interface ISystemContext : IDisposable {
		IDbConfig Config { get; }

		IStoreSystem StoreSystem { get; }

		ILogger Logger { get; }

		IRoutineResolver RoutineResolver { get; }

		// TypesManager TypesManager { get; }


		void Init(IDbConfig config);
	}
}