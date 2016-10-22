using System;

using Deveel.Data.Build;
using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Types {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTypesFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Types)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate));
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			systemQuery.Access().CreateTable(table => table
				.Named(TypeManager.TypeTableName)
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("parent", PrimitiveTypes.String())
				.WithColumn("sealed", PrimitiveTypes.Boolean())
				.WithColumn("abstract", PrimitiveTypes.Boolean())
				.WithColumn("owner", PrimitiveTypes.String()));

			systemQuery.Access().CreateTable(table => table
				.Named(TypeManager.TypeMemberTableName)
				.WithColumn("type_id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.String()));

			systemQuery.Access().AddPrimaryKey(TypeManager.TypeTableName, new[] { "id" }, "PK_TYPE");
			systemQuery.Access()
				.AddForeignKey(TypeManager.TypeMemberTableName, new[] {"type_id"}, TypeManager.TypeTableName, new[] {"id"},
					ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, "FK_MEMBER_TYPE");
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<TypeManager>()
					.HavingKey(DbObjectType.Type)
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<TypesTableContainer>()
					.InTransactionScope())
				//.Use<ITableCompositeCreateCallback>(options => options
				//	.With<TypesInit>()
				//	.InTransactionScope())
				.Use<ITypeResolver>(options => options
					.With<TypeManager>()
					.InTransactionScope())
				.Use<IRoutineResolver>(options => options
					.With<TypeManager>()
					.InTransactionScope());
		}
	}
}