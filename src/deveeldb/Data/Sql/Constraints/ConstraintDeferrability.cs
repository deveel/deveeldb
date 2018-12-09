using System;

namespace Deveel.Data.Sql.Constraints {
	/// <summary>
	/// The deferrability of a constraint
	/// </summary>
	/// <seealso cref="ConstraintInfo"/>
	public enum ConstraintDeferrability {
		/// <summary>
		/// The constraint is checked at the <c>COMMIT</c>
		/// of each transaction.
		/// </summary>
		InitiallyDeferred = 4,
		
		/// <summary>
		/// The constraint is checked immediately after
		/// each single statement.
		/// </summary>
		InitiallyImmediate = 5,
		
		/// <summary>
		/// A constraint whose check cannot be deferred to the
		/// <c>COMMIT</c> of a transaction.
		/// </summary>
		NotDeferrable = 6,
	}
}