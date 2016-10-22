using System;

using Deveel.Data.Build;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseRoutinesFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Routines)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnDatabaseCreate(OnDatabaseCreate)
				.OnTableCompositeSetup(OnCompositeSetup));
		}

		private static void OnCompositeSetup(IQuery systemQuery) {
			Create(systemQuery);
			AddForeignKeys(systemQuery);
		}

		private static void GrantToPublic(IQuery systemQuery) {
			systemQuery.Access().GrantOnTable(RoutineManager.RoutineTableName, User.PublicName, PrivilegeSets.TableRead);
			systemQuery.Access().GrantOnTable(RoutineManager.RoutineParameterTableName, User.PublicName, PrivilegeSets.TableRead);
		}

		private static void Create(IQuery systemQuery) {
			// SYSTEM.ROUTINE
			var tableInfo = new TableInfo(RoutineManager.RoutineTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("location", PrimitiveTypes.String());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo.AddColumn("return_type", PrimitiveTypes.String());
			tableInfo.AddColumn("username", PrimitiveTypes.String());
			systemQuery.Access().CreateTable(tableInfo);

			// SYSTEM.ROUTINE_PARAM
			tableInfo = new TableInfo(RoutineManager.RoutineParameterTableName);
			tableInfo.AddColumn("routine_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("arg_name", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_type", PrimitiveTypes.String());
			tableInfo.AddColumn("arg_attrs", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("in_out", PrimitiveTypes.Integer());
			tableInfo.AddColumn("offset", PrimitiveTypes.Integer());
			systemQuery.Access().CreateTable(tableInfo);
		}

		private static void AddForeignKeys(IQuery systemQuery) {
			var fkCol = new[] { "routine_id" };
			var refCol = new[] { "id" };
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;

			systemQuery.Access().AddForeignKey(RoutineManager.RoutineParameterTableName, fkCol, RoutineManager.RoutineTableName, refCol,
				onDelete, onUpdate, "ROUTINE_PARAMS_FK");
		}


		private static void OnDatabaseCreate(IQuery systemQuery) {
			GrantToPublic(systemQuery);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<RoutineManager>()
					.HavingKey(DbObjectType.Routine)
					.InTransactionScope())
				.Use<IRoutineResolver>(options => options
					.With<SystemFunctionsProvider>()
					.InDatabaseScope())
				.Use<IRoutineResolver>(options => options
					.With<RoutineManager>()
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<RoutinesTableContainer>()
					.InTransactionScope());

			//builder.Use<ITableCompositeSetupCallback>(options => options
			//	.With<RoutinesInit>()
			//	.InQueryScope());

			//builder.Use<IDatabaseCreateCallback>(options => options
			//	.With<RoutinesInit>()
			//	.InQueryScope());
		}
	}
}
