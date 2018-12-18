using System;
using System.Data;

namespace Deveel.Data {
	/// <summary>
	/// A feature to be activated in a database system
	/// </summary>
	/// <remarks>
	/// Implementations of this interface are invoked during
	/// the creation and setup of the components for the feature
	/// </remarks>
	public interface ISystemFeature {
		string Name { get; }

		Version Version { get; }

		/// <summary>
		/// Creates the components of the feature in the
		/// underlying system
		/// </summary>
		/// <param name="session">The system session that is
		/// used to access the underlying database system</param>
		void OnSystemCreate(ISession session);

		/// <summary>
		/// Sets up the components created for the feature
		/// in the underlying system
		/// </summary>
		/// <param name="session"></param>
		void OnSystemSetup(ISession session);
	}
}