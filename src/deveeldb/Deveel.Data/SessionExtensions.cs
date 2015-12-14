using System;

namespace Deveel.Data {
	public static class SessionExtensions {
		public static IDatabase Database(this ISession session) {
			return session.Transaction.Database;
		}
	}
}
