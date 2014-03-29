using System;

using Deveel.Data.Control;
using Deveel.Data.Functions;
using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public interface ISystemContext : IDisposable {
		IDbConfig Config { get; }

		IStoreSystem StoreSystem { get; }

		ILogger Logger { get; }

		IFunctionLookup FunctionLookup { get; }


		void Init(IDbConfig config);
	}
}