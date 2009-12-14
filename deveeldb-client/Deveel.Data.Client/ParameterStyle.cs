using System;

namespace Deveel.Data.Client {
	/// <summary>
	/// Defines the style used in a command to define the kind of 
	/// parameters permitted.
	/// </summary>
	public enum ParameterStyle {
		/// <summary>
		/// A marker parameter is represented by a question mark (<c>?</c>) 
		/// in the text of the command.
		/// </summary>
		Marker = 1,

		/// <summary>
		/// When using this style, parameters are represented by names prefixed
		/// by a <c>@</c> symbol.
		/// </summary>
		Named = 2
	}
}