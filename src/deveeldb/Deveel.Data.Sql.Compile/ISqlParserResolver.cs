using System;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// An object used to resolve a parser for a specific
	/// dialect as configured within the system.
	/// </summary>
	public interface ISqlParserResolver {
		/// <summary>
		/// Resolves a SQL parser that is associated
		/// to the given dialect.
		/// </summary>
		/// <param name="dialect">The name of the dialect to
		/// resolve a valid SQL parser.</param>
		/// <returns>
		/// Returns an instance of <see cref="ISqlParser"/>
		/// configured on the system for the given dialect name,
		/// or <c>null</c> if none parser was associated to the
		/// given name.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the specified <paramref name="dialect"/> is <c>null</c>
		/// or an empty string.
		/// </exception>
		ISqlParser ResolveParser(string dialect);
	}
}
