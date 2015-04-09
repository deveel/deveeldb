using System;

namespace Deveel.Data.Transactions {
	public static class TransactionSettingKeys {
		public const string IsolationLevel = "isolation level";
		public const string IgnoreIdentifiersCase = "ignore identifiers case";
		public const string ReadOnly = "read only";
		public const string AutoCommit = "autocommit";
		public const string CurrentSchema = "current schema";
		public const string ErrorOnDirtySelect = "error on dirty select";
	}
}