using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	public interface ISystem : IDatabaseHandler, IEventSource, IDisposable {
		IEnumerable<ModuleInfo> Modules { get; }
		 
		new ISystemContext Context { get; }

		IEnumerable<string> GetDatabases();
			
		IDatabase CreateDatabase(IConfiguration configuration, string adminUser, string adminPassword);

		bool DatabaseExists(string databaseName);

		IDatabase OpenDatabase(IConfiguration configuration);

		bool DeleteDatabase(string databaseName);
	}
}
