using System;
using System.Data;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Client {
	public static class DatabaseExtensions {
		public static IDbConnection CreateDbConnection(this IDatabase database, string userName, string password) {
			throw new NotImplementedException();
		}
	}
}
