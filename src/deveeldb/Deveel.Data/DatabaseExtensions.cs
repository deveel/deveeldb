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
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public static class DatabaseExtensions {
		public static string Name(this IDatabase database) {
			return database.DatabaseContext.DatabaseName();
		}

		public static Locker Locker(this IDatabase database) {
			return database.DatabaseContext.Locker;
		}

		#region Transactions

		public static ITransaction CreateTransaction(this IDatabase database, IsolationLevel isolation) {
			if (!database.IsOpen)
				throw new InvalidOperationException(String.Format("Database '{0}' is not open.", database.Name()));

			return database.CreateSafeTransaction(isolation);
		}

		public static ITransaction FindTransactionById(this IDatabase database, int commidId) {
			return database.TransactionFactory.OpenTransactions.FindById(commidId);
		}

		internal static ITransaction CreateSafeTransaction(this IDatabase database, IsolationLevel isolation) {
			return database.TransactionFactory.CreateTransaction(isolation);
		}

		#endregion

		#region Sessions

		static IUserSession CreateUserSession(this IDatabase database, SessionInfo sessionInfo) {
			if (sessionInfo == null)
				throw new ArgumentNullException("sessionInfo");

			// TODO: if the isolation is not specified, use a configured default one
			var isolation = sessionInfo.Isolation;
			if (isolation == IsolationLevel.Unspecified)
				isolation = IsolationLevel.Serializable;

			if (sessionInfo.CommitId >= 0)
				throw new ArgumentException("Cannot create a session that reference an existing commit state.");

			var transaction = database.CreateTransaction(isolation);
			return new UserSession(database, transaction, sessionInfo);
		}

		static IUserSession CreateSystemSession(this IDatabase database, IsolationLevel isolation) {
			var transaction = database.CreateTransaction(isolation);
			return new SystemUserSession(database, transaction);
		}

		internal static IUserSession CreateInitialSystemSession(this IDatabase database) {
			var transaction = database.CreateSafeTransaction(IsolationLevel.Serializable);
			return new SystemUserSession(database, transaction);
		}

		internal static IUserSession CreateSystemSession(this IDatabase database) {
			return database.CreateSystemSession(IsolationLevel.Serializable);
		}

		internal static IUserSession CreateUserSession(this IDatabase database, User user) {
			// TODO: get the pre-configured default transaction isolation
			return database.CreateUserSession(new SessionInfo(user));
		}

		private static IUserSession CreateUserSession(this IDatabase database, string userName, string password) {
			var user = database.Authenticate(userName, password);
			if (user == null)
				throw new InvalidOperationException(String.Format("Unable to create a session for user '{0}': not authenticated.", userName));

			return database.CreateUserSession(user);
		}

		static IUserSession OpenUserSession(this IDatabase database, SessionInfo sessionInfo) {
			if (sessionInfo == null)
				throw new ArgumentNullException("sessionInfo");

			var commitId = sessionInfo.CommitId;

			if (commitId < 0)
				throw new ArgumentException("Invalid commit reference specified.");

			var transaction = database.FindTransactionById(commitId);
			if (transaction == null)
				throw new InvalidOperationException(String.Format("The request transaction with ID '{0}' is not open.", commitId));

			return new UserSession(database, transaction, sessionInfo);
		}

		#endregion

		#region Query Contest

		public static IQueryContext CreateQueryContext(this IDatabase database, string userName, string password) {
			var session = database.CreateUserSession(userName, password);
			return new SessionQueryContext(session);
		}

		#endregion

		#region Security

		public static void CreateAdminUser(this IDatabase database, IQueryContext context, string adminName, string adminPassword) {
			try {
				var user = context.CreateUser(adminName, adminPassword);

				// This is the admin user so add to the 'secure access' table.
				context.AddUserToGroup(adminName, SystemGroups.SecureGroup);

				//context.GrantHostAccessToUser(adminName, KnownConnectionProtocols.TcpIp, "%");
				//context.GrantHostAccessToUser(adminName, KnownConnectionProtocols.Local, "%");

				context.GrantToUserOnSchema(database.DatabaseContext.DefaultSchema(), user.Name, Privileges.SchemaAll, true);
				context.GrantToUserOnSchema(SystemSchema.Name, user.Name, Privileges.SchemaRead);
				context.GrantToUserOnSchema(InformationSchema.SchemaName, user.Name, Privileges.SchemaRead);
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSystemException("Could not create the database administrator.", ex);
			}
		}

		public static User Authenticate(this IDatabase database, string username, string password) {
			// Create a temporary connection for authentication only...
			using (var session = database.CreateSystemSession()) {
				session.CurrentSchema(SystemSchema.Name);

				using (var queryContext = new SessionQueryContext(session)) {
					return queryContext.Authenticate(username, password);
				}
			}
		}

		#endregion
	}
}
