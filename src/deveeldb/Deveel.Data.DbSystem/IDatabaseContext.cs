using System;

using Deveel.Data.Security;
using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseContext : ISystemContext {
		event EventHandler OnShutdown;

		LoggedUsers LoggedUsers { get; }

		bool HasShutdown { get; }

		bool IsExecutingCommands { get; set; }

		StatementCache StatementCache { get; }


		IDatabase GetDatabase(string name);

		void RegisterDatabase(IDatabase database);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="block"></param>
		void Shutdown(bool block);
	}
}