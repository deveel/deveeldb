using System;

using Deveel.Data.Build;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Expressions.Build;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Schemas {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSchemaFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Schemata)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate)
				.OnDatabaseCreate(OnDatabaseCreate));
		}

		private static void OnDatabaseCreate(IQuery systemQuery) {
			InformationSchema.Create(systemQuery);
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			// SYSTEM.SCHEMA_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.SchemaInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.String())
				.WithColumn("culture", PrimitiveTypes.String())
				.WithColumn("other", PrimitiveTypes.String()));

			//var tableInfo = new TableInfo(SystemSchema.SchemaInfoTableName);
			//tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("name", PrimitiveTypes.String());
			//tableInfo.AddColumn("type", PrimitiveTypes.String());
			//tableInfo.AddColumn("culture", PrimitiveTypes.String());
			//tableInfo.AddColumn("other", PrimitiveTypes.String());
			//tableInfo = tableInfo.AsReadOnly();
			//systemQuery.Access().CreateTable(tableInfo);

			// TODO: Move this to the setup phase?
			CreateSystemSchema(systemQuery);
		}

		private static void CreateSchema(IQuery systemQuery, string name, string type) {
			systemQuery.Access().CreateSchema(new SchemaInfo(name, type));
		}

		private static void CreateSystemSchema(IQuery systemQuery) {
			CreateSchema(systemQuery, SystemSchema.Name, SchemaTypes.System);
			CreateSchema(systemQuery, InformationSchema.SchemaName, SchemaTypes.System);
			CreateSchema(systemQuery, systemQuery.Session.Database().Context.DefaultSchema(), SchemaTypes.Default);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
				.With<SchemaManager>()
				.HavingKey(DbObjectType.Schema)
				.InTransactionScope());
		}
	}
}