using System;

using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseContext : ISystemContext {
		event EventHandler OnShutdown;

		LoggedUsers LoggedUsers { get; }

		bool HasShutdown { get; }

		bool IsExecutingCommands { get; }


		IDatabase GetDatabase(string name);

		void RegisterDatabase(IDatabase database);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="block"></param>
		void Shutdown(bool block);
	}
}