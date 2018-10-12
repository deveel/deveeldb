using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The value that represents a SQL object value
	/// </summary>
	public interface ISqlValue : IComparable, IComparable<ISqlValue> {
		/// <summary>
		/// Checks if the current object is comparable with the given one.
		/// </summary>
		/// <param name="other">The other <see cref="ISqlValue"/> to compare.</param>
		/// <returns>
		/// Returns <c>true</c> if the current object is comparable
		/// with the given one, <c>false</c> otherwise.
		/// </returns>
		bool IsComparableTo(ISqlValue other);
	}
}
