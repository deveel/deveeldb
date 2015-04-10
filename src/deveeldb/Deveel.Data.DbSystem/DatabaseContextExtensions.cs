using System;

using Deveel.Data.Configuration;
using Deveel.Data.Store;

namespace Deveel.Data.DbSystem {
	public static class DatabaseContextExtensions {
		public static Type StorageSystemType(this IDatabaseContext context) {
			var value = context.Configuration.GetString(DatabaseConfigKeys.StorageSystem);
			if (String.IsNullOrEmpty(value))
				return null;

			if (String.Equals(value, DefaultStorageSystemNames.File))
				throw new NotImplementedException();

			if (String.Equals(value, DefaultStorageSystemNames.SingleFile))
				throw new NotSupportedException();

			if (string.Equals(value, DefaultStorageSystemNames.Heap))
				return typeof (InMemoryStorageSystem);

			return Type.GetType(value, false, true);
		}

		public static bool ReadOnly(this IDatabaseContext context) {
			return context.Configuration.GetBoolean(SystemConfigKeys.ReadOnly);
		}

		public static bool DeleteOnClose(this IDatabaseContext context) {
			return context.Configuration.GetBoolean(DatabaseConfigKeys.DeleteOnClose);
		}

		public static string DatabaseName(this IDatabaseContext context) {
			return context.Configuration.GetString(DatabaseConfigKeys.DatabaseName);
		}

		public static string DefaultSchema(this IDatabaseContext context) {
			return context.Configuration.GetString(DatabaseConfigKeys.DefaultSchema);
		}
	}
}
