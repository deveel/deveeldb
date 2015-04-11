using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseManager : IDisposable {
		ISystemContext SystemContext { get; }


		IEnumerable<string> DatabaseNames { get; }
			
		IDatabase CreateDatabase(IDbConfig config);

		IDatabase GetDatabase(IDbConfig config);

		bool DatabaseExists(string name);

		bool DropDatabase(string name);
	}
}
