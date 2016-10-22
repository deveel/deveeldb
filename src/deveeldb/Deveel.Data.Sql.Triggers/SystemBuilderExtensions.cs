using System;

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Triggers {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTriggersFeature(this ISystemBuilder builder) {
			return builder
				.UseFeature(feature => feature.Named(SystemFeatureNames.Triggers)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate));
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			systemQuery.Access().CreateTable(table => table
				.Named(TriggerManager.TriggerTableName)
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.Integer())
				.WithColumn("on_object", PrimitiveTypes.String())
				.WithColumn("time", PrimitiveTypes.Integer())
				.WithColumn("action", PrimitiveTypes.Integer())
				.WithColumn("procedure_name", PrimitiveTypes.String())
				.WithColumn("args", PrimitiveTypes.Binary())
				.WithColumn("body", PrimitiveTypes.Binary())
				.WithColumn("status", PrimitiveTypes.TinyInt()));
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<TriggerManager>()
					.HavingKey(DbObjectType.Trigger)
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<OldAndNewTableContainer>()
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<TriggersTableContainer>()
					.InTransactionScope());
		}
	}
}