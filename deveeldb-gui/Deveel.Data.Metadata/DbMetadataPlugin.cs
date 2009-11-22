using System;

using Deveel.Data.Plugins;

namespace Deveel.Data.Metadata {
	public sealed class DbMetadataPlugin : Plugin {
		public DbMetadataPlugin()
			: base("Database Metadata", "Displays the database schema tree.", 20) {
		}

		#region Overrides of Plugin

		public override void Init() {
			Services.RegisterSingletonComponent("DbMetadataForm", typeof(IDbMetadataProvider), typeof(DbMetadataForm));

			IHostWindow hostWindow = Services.HostWindow;
			hostWindow.AddPluginCommand(typeof(ShowDatabaseMetadataCommand));
			Services.CommandHandler.GetCommand(typeof(ShowDatabaseMetadataCommand)).Execute();
		}

		#endregion
	}
}