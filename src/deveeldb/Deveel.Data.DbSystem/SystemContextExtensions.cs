using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.DbSystem {
	public static class SystemContextExtensions {
		public static bool ReadOnly(this ISystemContext context) {
			return context.Configuration.GetBoolean(SystemConfigKeys.ReadOnly);
		}

		public static bool IgnoreCase(this ISystemContext context) {
			return context.Configuration.GetBoolean(SystemConfigKeys.IgnoreCase);
		}

		public static string DefaultSchema(this ISystemContext context) {
			return context.Configuration.GetString(SystemConfigKeys.DefaultSchema);
		}

		public static IDatabaseContext CreateDatabaseContext(this ISystemContext context, IDbConfig config, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			config.SetValue(DatabaseConfigKeys.DatabaseName, name);
			return context.CreateDatabaseContext(config);
		}

		public static IDatabaseContext GetDatabaseContext(this ISystemContext context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var config = DbConfig.Empty;
			context.Configuration.CopyTo(config);
			config.SetValue(DatabaseConfigKeys.DatabaseName, name);
			return context.GetDatabaseContext(config);
		}
	}
}
