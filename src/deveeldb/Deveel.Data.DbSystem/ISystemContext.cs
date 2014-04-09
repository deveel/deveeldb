using System;

using Deveel.Data.Configuration;
using Deveel.Data.Functions;
using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public interface ISystemContext : IDisposable {
		IDbConfig Config { get; }

		IStoreSystem StoreSystem { get; }

		ILogger Logger { get; }

		IFunctionLookup FunctionLookup { get; }

		// TypesManager TypesManager { get; }


		void Init(IDbConfig config);
	}
}