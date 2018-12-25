// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Schemata {
	public static class SystemSchema {
		public const string Name = "sys";

		public static readonly ObjectName SchemaName = new ObjectName(Name);

		public static readonly ObjectName SchemaInfoTableName = new ObjectName(SchemaName, "schema_info");

		public static readonly  ObjectName TableInfoTableName = new ObjectName(SchemaName, "table_info");

		public static readonly ObjectName TableColumnsTableName = new ObjectName(SchemaName, "table_cols");

		public static readonly ObjectName VariablesTableName = new ObjectName(SchemaName, "vars");

		public static readonly ObjectName ProductInfoTableName = new ObjectName(SchemaName, "product_info");

		public static readonly ObjectName StatisticsTableName = new ObjectName(SchemaName, "stats");

		public static readonly ObjectName PrimaryKeyInfoTableName = new ObjectName(SchemaName, "pkey_info");

		public static readonly ObjectName PrimaryKeyColumnsTableName = new ObjectName(SchemaName, "pkey_cols");

		public static readonly ObjectName ForeignKeyInfoTableName = new ObjectName(SchemaName, "fkey_info");

		public static readonly ObjectName ForeignKeyColumnsTableName = new ObjectName(SchemaName, "fkey_cols");

		public static readonly  ObjectName UniqueKeyInfoTableName = new ObjectName(SchemaName, "unique_info");

		public static readonly ObjectName UniqueKeyColumnsTableName = new ObjectName(SchemaName, "unique_cols");

		public static readonly ObjectName CheckInfoTableName = new ObjectName(SchemaName, "check_info");

		public static readonly ObjectName SqlTypesTableName = new ObjectName(SchemaName, "sql_types");

		public static readonly ObjectName OpenSessionsTableName = new ObjectName(SchemaName, "open_sessions");


		private static IEnumerable<ISystemFeature> GetAllFeatures(IDatabase database,
			IEnumerable<ISystemFeature> features) {
			var result = new List<ISystemFeature>();
			result.AddRange(database.Scope.ResolveAll<ISystemFeature>());

			if (features != null) {
				foreach (var feature in features) {
					var featureType = feature.GetType();
					if (!result.Any(x => featureType.IsInstanceOfType(x)))
						result.Add(feature);
				}
			}


			return result;
		}

		public static void Create(IDatabase database) {
			Create(database, null);
		}

		public static void Create(IDatabase database, IEnumerable<ISystemFeature> features) {
			features = GetAllFeatures(database, features);

			using (var session = database.CreateSystemSession(Name)) {
				try {
					foreach (var feature in features) {
						feature.OnSystemCreate(session);
					}
				} catch (Exception ex) {
					throw new DatabaseException("An error occurred while creating the system", ex);
				}
			}
		}

		public static void Setup(IDatabase database) {
			Setup(database, null);
		}

		public static void Setup(IDatabase database, IEnumerable<ISystemFeature> features) {
			features = GetAllFeatures(database, features);

			using (var session = database.CreateSystemSession(Name)) {
				try {
					foreach (var feature in features) {
						feature.OnSystemSetup(session);
					}
				} catch (Exception ex) {
					throw new DatabaseException("An error occurred while creating the system", ex);
				}
			}

		}
	}
}