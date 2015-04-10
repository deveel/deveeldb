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
			return database.CreateSession(User.System, ConnectionEndPoint.Embedded, isolation);
		}

		public static IUserSession CreateSystemSession(this IDatabase database) {
			return database.CreateSystemSession(TransactionIsolation.Serializable);
		}

		#region Security

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
