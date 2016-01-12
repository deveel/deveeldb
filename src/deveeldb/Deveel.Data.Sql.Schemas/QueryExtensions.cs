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

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Schemas {
	public static class QueryExtensions {
		public static void CreateSchema(this IQuery context, string name, string type) {
			if (!context.UserCanCreateSchema())
				throw new InvalidOperationException();      // TODO: throw a specialized exception

			context.CreateObject(new SchemaInfo(name, type));
		}

		public static void DropSchema(this IQuery context, string schemaName) {
			if (!context.UserCanDropSchema(schemaName))
				throw new MissingPrivilegesException(context.UserName(), new ObjectName(schemaName), Privileges.Drop);

			context.DropObject(DbObjectType.Schema, new ObjectName(schemaName));
		}

		public static bool SchemaExists(this IQuery context, string name) {
			return context.ObjectExists(DbObjectType.Schema, new ObjectName(name));
		}

		public static ObjectName ResolveSchemaName(this IQuery context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			return context.ResolveObjectName(DbObjectType.Schema, new ObjectName(name));
		}
	}
}
