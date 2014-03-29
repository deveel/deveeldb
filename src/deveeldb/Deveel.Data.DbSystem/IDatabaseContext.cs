using System;

using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseContext : ISystemContext {
		UserManager UserManager { get; }


		IDatabase GetDatabase(string name);

		void RegisterDatabase(IDatabase database);
	}
}