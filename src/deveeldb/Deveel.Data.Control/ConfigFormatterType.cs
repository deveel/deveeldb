using System;

namespace Deveel.Data.Control {
	/// <summary>
	/// Enumerates the <see cref="IConfigFormatter"/> implementations
	/// provided by the framework.
	/// </summary>
	/// <remarks>
	/// This enumeration is a formalization of the implementations
	/// already provided within the framework: it is possible to
	/// format the configuration input/output by implementing the
	/// interface <see cref="IConfigFormatter"/> and passing one
	/// of its instance to <see cref="DbConfig.LoadFrom(System.IO.Stream,Deveel.Data.Control.IConfigFormatter)"/>
	/// and <see cref="DbConfig.SaveTo(System.IO.Stream, IConfigFormatter)"/>
	/// </remarks>
	/// <seealso cref="IConfigFormatter"/>
	public enum ConfigFormatterType {
		/// <summary>
		/// Plain key/value pairs
		/// </summary>
		Properties,

		/// <summary>
		/// XML formatted configuration
		/// </summary>
		Xml,

		/// <summary>
		/// Key/value entries stored into the application 
		/// AppSettings configuration section.
		/// </summary>
		AppSettings
	}
}