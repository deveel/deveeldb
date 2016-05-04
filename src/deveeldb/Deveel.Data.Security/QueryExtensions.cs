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

using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Security {
	static class QueryExtensions {
		public static void CreateAdminUser(this IQuery context, string userName, string identification, string token) {
			try {
				context.Access().CreateUser(userName, identification, token);

				// This is the admin user so add to the 'secure access' table.
				context.Access().AddUserToRole(userName, SystemRoles.SecureAccessRole);

				context.Access().GrantOnSchema(context.Session.Database().Context.DefaultSchema(), userName, PrivilegeSets.SchemaAll, true);
				context.Access().GrantOnSchema(SystemSchema.Name, userName, PrivilegeSets.SchemaRead);
				context.Access().GrantOnSchema(InformationSchema.SchemaName, userName, PrivilegeSets.SchemaRead);
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSystemException("Could not create the database administrator.", ex);
			}
		}

		public static void CreateAdminUser(this IQuery query, string userName, string password) {
			query.CreateAdminUser(userName, KnownUserIdentifications.ClearText, password);
		}
	}
}
