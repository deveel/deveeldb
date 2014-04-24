using System;

using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseContext : ISystemContext {
		LoggedUsers LoggedUsers { get; }


		IDatabase GetDatabase(string name);

		void RegisterDatabase(IDatabase database);
	}
}