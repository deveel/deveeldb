using System;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Describes the functionalities of a database within the system
	/// </summary>
	public interface IDatabase : IDisposable {
		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		string Name { get; }

		bool Exists { get; }

		bool IsInitialized { get; }

		IDatabaseContext Context { get; }

		UserManager UserManager { get; }

		bool DeleteOnShutdown { get; set; }

		Table SingleRowTable { get; }

		Log CommandsLog { get; }

		TableDataConglomerate Conglomerate { get; }


		void Create(string adminUser, string adminPass);

		void Init();

		IDatabaseConnection CreateNewConnection(User user, TriggerCallback triggerCallback);

		void Shutdown();
	}
}
