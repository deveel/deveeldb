// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
			systemQuery.Access().CreateTable(table => table
				.Named(SequenceManager.SequenceInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.Numeric()));

			//var tableInfo = new TableInfo(SequenceManager.SequenceInfoTableName);
			//tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("schema", PrimitiveTypes.String());
			//tableInfo.AddColumn("name", PrimitiveTypes.String());
			//tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			//tableInfo = tableInfo.AsReadOnly();
			//systemQuery.Access().CreateTable(tableInfo);

			// SYSTEM.SEQUENCE
			systemQuery.Access().CreateTable(table => table
				.Named(SequenceManager.SequenceTableName)
				.WithColumn("seq_id", PrimitiveTypes.Numeric())
				.WithColumn("last_value", PrimitiveTypes.Numeric())
				.WithColumn("increment", PrimitiveTypes.Numeric())
				.WithColumn("minvalue", PrimitiveTypes.Numeric())
				.WithColumn("maxvalue", PrimitiveTypes.Numeric())
				.WithColumn("start", PrimitiveTypes.Numeric())
				.WithColumn("cache", PrimitiveTypes.Numeric())
				.WithColumn("cycle", PrimitiveTypes.Boolean()));

			//tableInfo = new TableInfo(SequenceManager.SequenceTableName);
			//tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("increment", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("start", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("cache", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean());
			//tableInfo = tableInfo.AsReadOnly();
			//systemQuery.Access().CreateTable(tableInfo);
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