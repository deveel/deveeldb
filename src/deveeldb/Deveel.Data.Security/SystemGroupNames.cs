using System;

namespace Deveel.Data.Security {
	public static class SystemGroupNames {
		/// <summary>
		/// The name of the lock group.
		/// </summary>
		/// <remarks>
		/// If a user belongs to this group the user account is locked and they are not 
		/// allowed to log into the database.
		/// </remarks>
		public const string LockGroup = "#locked";

		/// <summary>
		/// The name of the schema manager group.
		/// </summary>
		/// <remarks>
		/// Users that belong in this group can create and drop schema from the system.
		/// </remarks>
		public const String SchemaManagerGroup = "schema manager";

		/// <summary>
		/// THe name of the secure access group.
		/// </summary>
		/// <remarks>
		/// If a user belongs to this group they are permitted to perform a number of 
		/// priviledged operations such as shutting down the database, and adding and 
		/// removing users.
		/// </remarks>
		public const string SecureGroup = "secure access";

		/// <summary>
		/// The name of the user manager group.
		/// </summary>
		/// <remarks>
		/// Users that belong in this group can create, alter and drop users from the 
		/// system.
		/// </remarks>
		public const String UserManagerGroup = "user manager";
	}
}
