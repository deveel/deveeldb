using System;

using Deveel.Data.Configuration;
using Deveel.Data.Store;

namespace Deveel.Data.DbSystem {
	public static class DatabaseConfigKeys {
		public const string DatabaseNameKeyName = "database.name";
		public const string DefaultSchemKeyName = "database.defaultSchema";
		public const string DeleteOnCloseKeyName = "database.deleteOnClose";

		public const string StoreSystemKeyName = "store.type";
		public const string FileStoreBasePathKeyName = "store.file.basePath";
		public const string FileStoreMaxFilesKeyName = "store.file.maxFiles";

		public static readonly ConfigKey DatabaseName = new ConfigKey(DatabaseNameKeyName, typeof(string));

		public static readonly ConfigKey DefaultSchema = new ConfigKey(DefaultSchemKeyName, "APP", typeof(string));

		public static readonly ConfigKey StorageSystem = new ConfigKey(StoreSystemKeyName, DefaultStorageSystemNames.Heap, typeof(string));

		public static readonly ConfigKey DeleteOnClose = new ConfigKey(DeleteOnCloseKeyName, false, typeof(bool));
	}
}
