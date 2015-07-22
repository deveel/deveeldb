using System;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseHandler {
		IDatabase GetDatabase(string databaseName);
	}
}
