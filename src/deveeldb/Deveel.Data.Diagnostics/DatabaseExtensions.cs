using System;

namespace Deveel.Data.Diagnostics {
	public static class DatabaseExtensions {
		public static void OnSessionEvent(this IDatabase database, string userName, int commitId, SessionEventType eventType) {
			database.OnEvent(new SessionEvent(userName, commitId, eventType));
		}

		public static void OnSessionBegin(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.Begin);
		}

		public static void OnSessionCommit(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.EndForCommit);
		}

		public static void OnSessionRollback(this IDatabase database, string userName, int commitId) {
			database.OnSessionEvent(userName, commitId, SessionEventType.EndForRollback);
		}
	}
}
