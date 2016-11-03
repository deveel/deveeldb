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
			systemQuery.Access().CreateTable(table => table
				.Named(TriggerManager.TriggerTableName)
				.WithColumn("schema", PrimitiveTypes.String())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.Integer())
				.WithColumn("on_object", PrimitiveTypes.String())
				.WithColumn("time", PrimitiveTypes.Integer())
				.WithColumn("action", PrimitiveTypes.Integer())
				.WithColumn("procedure_name", PrimitiveTypes.String())
				.WithColumn("args", PrimitiveTypes.Binary())
				.WithColumn("body", PrimitiveTypes.Binary())
				.WithColumn("status", PrimitiveTypes.TinyInt()));
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder
				.Use<IObjectManager>(options => options
					.With<TriggerManager>()
					.HavingKey(DbObjectType.Trigger)
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<OldAndNewTableContainer>()
					.InTransactionScope())
				.Use<ITableContainer>(options => options
					.With<TriggersTableContainer>()
					.InTransactionScope());
		}
	}
}