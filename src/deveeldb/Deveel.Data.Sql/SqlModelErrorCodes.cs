using System;

namespace Deveel.Data.Sql {
	public static class SqlModelErrorCodes {
		/// <summary>
		/// A Primary Key constraint violation error code.
		/// </summary>
		public const int PrimaryKeyViolation = 20;

		/// <summary>
		/// A Unique constraint violation error code.
		/// </summary>
		public const int UniqueViolation = 21;

		/// <summary>
		/// A Check constraint violation error code.
		/// </summary>
		public const int CheckViolation = 22;

		/// <summary>
		/// A Foreign Key constraint violation error code.
		/// </summary>
		public const int ForeignKeyViolation = 23;

		/// <summary>
		/// A Nullable constraint violation error code (data added to not null
		/// columns that was null).
		/// </summary>
		public const int NullableViolation = 24;

		/// <summary>
		/// Type constraint violation error code (tried to insert an object
		/// that wasn't derived from the object type defined for the column).
		/// </summary>
		public const int ObjectTypeViolation = 25;

		/// <summary>
		/// Tried to drop a table that is referenced by another source.
		/// </summary>
		public const int DropTableViolation = 26;

		/// <summary>
		/// Column can't be dropped before of an reference to it.
		/// </summary>
		public const int DropColumnViolation = 27;
	}
}
