using System;

namespace Deveel.Data.Configuration {
	/// <summary>
	/// Marks a component as configurable and passes the configuration
	/// object that is used to load the configurations handled.
	/// </summary>
	public interface IConfigurable {
		/// <summary>
		/// Gets a value indicating whether this instance was configured.
		/// </summary>
		/// <remarks>
		/// This information prevents from calling the <see cref="Configure"/>
		/// method more than once.
		/// </remarks>
		/// <value>
		/// <c>true</c> if this instance is configured; otherwise, <c>false</c>.
		/// </value>
		bool IsConfigured { get; }

		/// <summary>
		/// Configures the component with the settings provided
		/// by the specified configuration object.
		/// </summary>
		/// <param name="config">The container of settings used to
		/// configure the object</param>
		/// <seealso cref="IConfiguration"/>
		void Configure(IConfiguration config);
	}
}