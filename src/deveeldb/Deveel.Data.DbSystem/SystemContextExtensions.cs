// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Configuration;
using Deveel.Data.Spatial;

namespace Deveel.Data.DbSystem {
	public static class SystemContextExtensions {
		public static bool ReadOnly(this ISystemContext context) {
			return context.Configuration.GetBoolean(SystemConfigKeys.ReadOnly);
		}

		public static bool IgnoreCase(this ISystemContext context) {
			return context.Configuration.GetBoolean(SystemConfigKeys.IgnoreCase);
		}

		public static string DefaultSchema(this ISystemContext context) {
			return context.Configuration.GetString(SystemConfigKeys.DefaultSchema);
		}

		public static ISpatialContext SpatialContext(this ISystemContext context) {
			return context.ServiceProvider.Resolve<ISpatialContext>();
		}

		//public static IDatabaseContext CreateDatabaseContext(this ISystemContext context, IDbConfig config, string name) {
		//	if (String.IsNullOrEmpty(name))
		//		throw new ArgumentNullException("name");

		//	config.SetValue(DatabaseConfigKeys.DatabaseName, name);
		//	return context.CreateDatabaseContext(config);
		//}

		//public static IDatabaseContext GetDatabaseContext(this ISystemContext context, string name) {
		//	if (String.IsNullOrEmpty(name))
		//		throw new ArgumentNullException("name");

		//	var config = DbConfig.Empty;
		//	context.Configuration.CopyTo(config);
		//	config.SetValue(DatabaseConfigKeys.DatabaseName, name);
		//	return context.GetDatabaseContext(config);
		//}
	}
}
