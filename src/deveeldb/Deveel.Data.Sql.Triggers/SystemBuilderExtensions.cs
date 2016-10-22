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
			var tableInfo = new TableInfo(TriggerManager.TriggerTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Integer());
			tableInfo.AddColumn("on_object", PrimitiveTypes.String());
			tableInfo.AddColumn("time", PrimitiveTypes.Integer());
			tableInfo.AddColumn("action", PrimitiveTypes.Integer());
			tableInfo.AddColumn("procedure_name", PrimitiveTypes.String());
			tableInfo.AddColumn("args", PrimitiveTypes.Binary());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo.AddColumn("status", PrimitiveTypes.TinyInt());
			systemQuery.Access().CreateTable(tableInfo);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<TriggerManager>()
					.HavingKey(DbObjectType.Trigger)
					.InTransactionScope())
				//.Use<ITableCompositeCreateCallback>(options => options
				//	.With<TriggersInit>()
				//	.InQueryScope())
				.Use<ITableContainer>(options => options
					.With<OldAndNewTableContainer>()
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<TriggersTableContainer>()
					.InTransactionScope());
		}
	}
}