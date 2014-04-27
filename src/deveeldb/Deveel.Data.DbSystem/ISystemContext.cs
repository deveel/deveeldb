using System;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Store;
using Deveel.Data.Text;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public interface ISystemContext : IDisposable {
		IDbConfig Config { get; }

		IStoreSystem StoreSystem { get; }

		ILogger Logger { get; }

		Stats Stats { get; }

		IRoutineResolver RoutineResolver { get; }

		IRegexLibrary RegexLibrary { get; }

		// TypesManager TypesManager { get; }

		DataCellCache DataCellCache { get; }


		void Init(IDbConfig config);

		object CreateEvent(EventHandler handler);

		void PostEvent(int waitTime, object e);
	}
}