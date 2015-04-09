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
	}
}
