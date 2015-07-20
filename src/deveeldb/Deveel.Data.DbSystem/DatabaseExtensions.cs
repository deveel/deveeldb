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

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public static class DatabaseExtensions {
		public static string Name(this IDatabase database) {
			return database.Context.DatabaseName();
		}

		public static IUserSession CreateSystemSession(this IDatabase database, TransactionIsolation isolation) {
			var transaction = database.CreateTransaction(isolation);
			return new SystemUserSession(database, transaction);
		}

		public static IUserSession CreateSystemSession(this IDatabase database) {
			return database.CreateSystemSession(TransactionIsolation.Serializable);
		}

		public static IUserSession CreateSession(this IDatabase database, User user) {
			// TODO: get the pre-configured default transaction isolation
			return database.CreateSession(new SessionInfo(user));
		}

		public static IUserSession CreateSession(this IDatabase database, string userName, string password) {
			return CreateSession(database, userName, password, ConnectionEndPoint.Embedded);
		}

		public static IUserSession CreateSession(this IDatabase database, string userName, string password, ConnectionEndPoint endPoint) {
			var user = database.Authenticate(userName, password, endPoint);
			if (user == null)
				throw new InvalidOperationException(String.Format("Unable to create a session for user '{0}': not authenticated.", userName));

			return database.CreateSession(user);
		}

		#region Security

		public static void CreateAdminUser(this IDatabase database, IQueryContext context, string adminName, string adminPassword) {
			try {
				var user = context.CreateUser(adminName, adminPassword);

				// This is the admin user so add to the 'secure access' table.
				context.AddUserToGroup(adminName, SystemGroupNames.SecureGroup);

				context.GrantHostAccessToUser(adminName, KnownConnectionProtocols.TcpIp, "%");
				context.GrantHostAccessToUser(adminName, KnownConnectionProtocols.Local, "%");

				context.GrantToUserOnSchema(database.Context.DefaultSchema(), user, User.System, Privileges.SchemaAll, true);
				context.GrantToUserOnSchema(SystemSchema.Name, user, User.System, Privileges.SchemaRead);

				// TODO: Grant READ on INFORMATION_SCHEMA

				context.GrantToUserOnSchemaTables(SystemSchema.Name, user, User.System, Privileges.TableRead);
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSystemException("Could not create the database administrator.", ex);
			}
		}

		public static User Authenticate(this IDatabase database, string username, string password) {
			return Authenticate(database, username, password, ConnectionEndPoint.Embedded);
		}

		public static User Authenticate(this IDatabase database, string username, string password, ConnectionEndPoint endPoint) {
			// Create a temporary connection for authentication only...
			using (var session = database.CreateSystemSession()) {
				session.CurrentSchema(SystemSchema.Name);
				session.ExclusiveLock();

				using (var queryContext = new SessionQueryContext(session)) {
					return queryContext.Authenticate(username, password, endPoint);
				}
			}
		}

		#endregion
	}
}
