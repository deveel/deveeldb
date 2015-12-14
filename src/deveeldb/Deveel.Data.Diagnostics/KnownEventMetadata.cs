using System;

namespace Deveel.Data.Diagnostics {
	public static class KnownEventMetadata {
		public const string UserName = "session.userName";
		public const string SessionStartTime = "session.startTime";
		public const string LastCommandTime = "session.lastCommandTime";
		public const string CommitId = "transaction.commitId";
		public const string CurrentSchema = "transaction.currentSchema";
		public const string IsolationLevel = "transaction.isolationLevel";
		public const string IgnoreIdentifiersCase = "transaction.ignoreIdCase";
		public const string ReadOnlyTransaction = "transaction.readOnly";
		public const string DatabaseName = "database.name";
		public const string SessionCount = "database.sessionCount";
		public const string TableId = "table.id";
		public const string TableName = "table.name";
	}
}
