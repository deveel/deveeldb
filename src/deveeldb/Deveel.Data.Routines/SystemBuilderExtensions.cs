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
			systemQuery.Access()
				.CreateTable(table => table
					.Named(RoutineManager.RoutineTableName)
					.WithColumn("id", PrimitiveTypes.Numeric())
					.WithColumn("schema", PrimitiveTypes.String())
					.WithColumn("name", PrimitiveTypes.String())
					.WithColumn("type", PrimitiveTypes.String())
					.WithColumn("location", PrimitiveTypes.String())
					.WithColumn("body", PrimitiveTypes.Binary())
					.WithColumn("return_type", PrimitiveTypes.String())
					.WithColumn("username", PrimitiveTypes.String()));


			// SYSTEM.ROUTINE_PARAM
			systemQuery.Access().CreateTable(table => table
				.Named(RoutineManager.RoutineParameterTableName)
				.WithColumn("routine_id", PrimitiveTypes.Numeric())
				.WithColumn("arg_name", PrimitiveTypes.String())
				.WithColumn("arg_type", PrimitiveTypes.String())
				.WithColumn("arg_attrs", PrimitiveTypes.Numeric())
				.WithColumn("in_out", PrimitiveTypes.Integer())
				.WithColumn("offset", PrimitiveTypes.Integer()));
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
