using System;

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseTablesFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Tables)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate)
				.OnTableCompositeSetup(OnCompositeSetup));
		}

		private static void OnCompositeSetup(IQuery systemQuery) {
			// -- Primary Keys --
			// The 'id' columns are primary keys on all the system tables,
			var idCol = new[] { "id" };
			systemQuery.Access().AddPrimaryKey(SystemSchema.PrimaryKeyInfoTableName, idCol, "SYSTEM_PK_PK");
			systemQuery.Access().AddPrimaryKey(SystemSchema.ForeignKeyInfoTableName, idCol, "SYSTEM_FK_PK");
			systemQuery.Access().AddPrimaryKey(SystemSchema.UniqueKeyInfoTableName, idCol, "SYSTEM_UNIQUE_PK");
			systemQuery.Access().AddPrimaryKey(SystemSchema.CheckInfoTableName, idCol, "SYSTEM_CHECK_PK");
			systemQuery.Access().AddPrimaryKey(SystemSchema.SchemaInfoTableName, idCol, "SYSTEM_SCHEMA_PK");

			// -- Foreign Keys --
			// Create the foreign key references,
			var fkCol = new string[1];
			var fkRefCol = new[] { "id" };

			fkCol[0] = "pk_id";
			systemQuery.Access().AddForeignKey(SystemSchema.PrimaryKeyColumnsTableName, fkCol, SystemSchema.PrimaryKeyInfoTableName, fkRefCol, "SYSTEM_PK_FK");

			fkCol[0] = "fk_id";
			systemQuery.Access().AddForeignKey(SystemSchema.ForeignKeyColumnsTableName, fkCol, SystemSchema.ForeignKeyInfoTableName, fkRefCol, "SYSTEM_FK_FK");

			fkCol[0] = "un_id";
			systemQuery.Access().AddForeignKey(SystemSchema.UniqueKeyColumnsTableName, fkCol, SystemSchema.UniqueKeyInfoTableName, fkRefCol, "SYSTEM_UNIQUE_FK");

			// pkey_info 'schema', 'table' column is a unique set,
			// (You are only allowed one primary key per table).
			var columns = new[] { "schema", "table" };
			systemQuery.Access().AddUniqueKey(SystemSchema.PrimaryKeyInfoTableName, columns, "SYSTEM_PKEY_ST_UNIQUE");

			// schema_info 'name' column is a unique column,
			columns = new String[] { "name" };
			systemQuery.Access().AddUniqueKey(SystemSchema.SchemaInfoTableName, columns, "SYSTEM_SCHEMA_UNIQUE");

			//    columns = new String[] { "name" };
			columns = new String[] { "name", "schema" };
			// pkey_info 'name' column is a unique column,
			systemQuery.Access().AddUniqueKey(SystemSchema.PrimaryKeyInfoTableName, columns, "SYSTEM_PKEY_UNIQUE");

			// fkey_info 'name' column is a unique column,
			systemQuery.Access().AddUniqueKey(SystemSchema.ForeignKeyInfoTableName, columns, "SYSTEM_FKEY_UNIQUE");

			// unique_info 'name' column is a unique column,
			systemQuery.Access().AddUniqueKey(SystemSchema.UniqueKeyInfoTableName, columns, "SYSTEM_UNIQUE_UNIQUE");

			// check_info 'name' column is a unique column,
			systemQuery.Access().AddUniqueKey(SystemSchema.CheckInfoTableName, columns, "SYSTEM_CHECK_UNIQUE");
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			// SYSTEM.PKEY_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.PrimaryKeyInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("table", PrimitiveTypes.String())
				.WithColumn("deferred", PrimitiveTypes.Numeric()));

			// SYSTEM.PKEY_COLS
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.PrimaryKeyColumnsTableName)
				.WithColumn("pk_id", PrimitiveTypes.Numeric())
				.WithColumn("column", PrimitiveTypes.String())
				.WithColumn("seq_no", PrimitiveTypes.Numeric()));

			// SYSTEM.FKEY_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.ForeignKeyInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("table", PrimitiveTypes.String())
				.WithColumn("ref_schema", PrimitiveTypes.String())
				.WithColumn("ref_table", PrimitiveTypes.String())
				.WithColumn("update_rule", PrimitiveTypes.Numeric())
				.WithColumn("delete_rule", PrimitiveTypes.Numeric())
				.WithColumn("deferred", PrimitiveTypes.Numeric()));

			// SYSTEM.FKEY_COLS
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.ForeignKeyColumnsTableName)
				.WithColumn("fk_id", PrimitiveTypes.Numeric())
				.WithColumn("fcolumn", PrimitiveTypes.String())
				.WithColumn("pcolumn", PrimitiveTypes.String())
				.WithColumn("seq_no", PrimitiveTypes.Numeric()));

			// SYSTEM.UNIQUE_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.UniqueKeyInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("table", PrimitiveTypes.String())
				.WithColumn("deferred", PrimitiveTypes.Numeric()));

			// SYSTEM.UNIQUE_COLS
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.UniqueKeyColumnsTableName)
				.WithColumn("un_id", PrimitiveTypes.Numeric())
				.WithColumn("column", PrimitiveTypes.String())
				.WithColumn("seq_no", PrimitiveTypes.Numeric()));

			// SYSTEM.CHECK_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.CheckInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("table", PrimitiveTypes.String())
				.WithColumn("expression", PrimitiveTypes.String())
				.WithColumn("deferred", PrimitiveTypes.Numeric())
				.WithColumn("serialized_expression", PrimitiveTypes.Binary()));
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<TableManager>()
					.HavingKey(DbObjectType.Table)
					.InTransactionScope());
		}
	}
}
