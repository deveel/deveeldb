using System;

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Sequences {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSequencesFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Sequences)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate));

			//builder.Use<ITableCompositeCreateCallback>(options => options
			//	.With<SequencesInit>()
			//	.InQueryScope());
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			// SYSTEM.SEQUENCE_INFO
			var tableInfo = new TableInfo(SequenceManager.SequenceInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			systemQuery.Access().CreateTable(tableInfo);

			// SYSTEM.SEQUENCE
			tableInfo = new TableInfo(SequenceManager.SequenceTableName);
			tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("increment", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("start", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cache", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean());
			tableInfo = tableInfo.AsReadOnly();
			systemQuery.Access().CreateTable(tableInfo);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<SequenceManager>()
					.HavingKey(DbObjectType.Sequence)
					.InTransactionScope())
				.Use<ITableContainer>(optiions => optiions
					.With<SequenceTableContainer>()
					.InTransactionScope());
		}
	}
}