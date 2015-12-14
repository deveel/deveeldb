using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Diagnostics {
	public static class SessionExtensions {
		public static void OnBegin(this ISession session) {
			session.Database().OnSessionBegin(session.User.Name, session.Transaction.CommitId);
		}

		public static void OnCommit(this ISession session) {
			session.Database().OnSessionCommit(session.User.Name, session.Transaction.CommitId);
		}

		public static void OnRollback(this ISession session) {
			session.Database().OnSessionRollback(session.User.Name, session.Transaction.CommitId);
		}

		public static void OnQuery(this ISession session, SqlQuery query) {
			session.OnEvent(new QueryEvent(query));
		}
	}
}
