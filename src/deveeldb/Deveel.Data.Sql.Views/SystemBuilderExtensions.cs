using System;

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Views {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseViewsFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Views)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate));
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			var tableInfo = new TableInfo(ViewManager.ViewTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("query", PrimitiveTypes.String());
			tableInfo.AddColumn("plan", PrimitiveTypes.Binary());

			// TODO: Columns...

			systemQuery.Access().CreateTable(tableInfo);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
					.With<ViewManager>()
					.InTransactionScope()
					.HavingKey(DbObjectType.View))
				//.Use<ITableCompositeCreateCallback>(options => options
				//	.With<ViewsInit>()
				//	.HavingKey("Views")
				//	.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<ViewTableContainer>()
					.InTransactionScope());
		}
	}
}