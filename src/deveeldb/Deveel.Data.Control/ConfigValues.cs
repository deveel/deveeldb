using System;

namespace Deveel.Data.Control {
	/// <summary>
	/// This static class provides default configuration values
	/// that can be set in a <see cref="DbConfig"/> instance,
	/// in association with keys defined in <see cref="ConfigKeys"/>
	/// to configure a database system.
	/// </summary>
	/// <remarks>
	/// The values of the fields in the class are simply a formal
	/// definition and they are not intended to be constraining
	/// in the free configuration of database system.
	/// </remarks>
	public static class ConfigValues {
		/// <summary>
		/// Associated to the key <see cref="ConfigKeys.StorageSystem"/>
		/// tells the system the database storage will be backed by the 
		/// file-system (default implementation)
		/// </summary>
		public const string FileStorageSystem = "file";

		/// <summary>
		/// Associated to the key <see cref="ConfigKeys.StorageSystem"/>
		/// tells the system the database storage will be backed by the 
		/// heap (default implementation)
		/// </summary>
		public const string HeapStorageSystem = "heap";
	}
}
