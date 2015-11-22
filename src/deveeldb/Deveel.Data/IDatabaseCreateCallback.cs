using System;

namespace Deveel.Data {
	/// <summary>
	/// Defines the callback that a <see cref="IDatabase.Create"/> function
	/// calls right before the finalization of the database initial state.
	/// </summary>
	/// <remarks>
	/// <para>
	/// External features will be able to implement a callback to create
	/// special objects after the core is generated, still in scope of the
	/// creation process.
	/// </para>
	/// <para>
	/// To activate this function the external features will also be
	/// required to register this to <see cref="ISystemContext.ServiceProvider"/>.
	/// </para>
	/// </remarks>
	public interface IDatabaseCreateCallback {
		/// <summary>
		/// Called when the database is created and before the
		/// finalization of the initialization process.
		/// </summary>
		/// <param name="context">The privileged system context that
		/// is used to generate the initial database.</param>
		void OnDatabaseCreate(IQuery context);
	}
}
