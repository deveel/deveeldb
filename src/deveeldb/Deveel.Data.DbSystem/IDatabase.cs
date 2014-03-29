using System;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Describes the functionalities of a database within the system
	/// </summary>
	public interface IDatabase : IDisposable {
		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// Returns true if this database is in read-only mode.
		/// </summary>
		bool IsReadOnly { get; }

		bool Exists { get; }

		IDatabaseContext Context { get; }
	}
}
