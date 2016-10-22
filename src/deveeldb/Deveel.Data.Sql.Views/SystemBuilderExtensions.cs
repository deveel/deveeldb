using System;

using Deveel.Data.Build;
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
			systemQuery.Access().CreateTable(table => table
				.Named(ViewManager.ViewTableName)
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("query", PrimitiveTypes.String())
				.WithColumn("plan", PrimitiveTypes.Binary()));

			// TODO: Columns...
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
					.With<ViewManager>()
					.InTransactionScope()
					.HavingKey(DbObjectType.View))
				.Use<ITableContainer>(options => options
					.With<ViewTableContainer>()
					.InTransactionScope());
		}
	}
}