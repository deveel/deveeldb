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

		public const string QueryPlannerKeyName = "query.planner.type";

		public const string CellCacheTypeKeyName = "cellCache.type";
		public const string CellCacheMaxSizeKeyName = "cellCache.maxSize";
		public const string CellCacheMaxCellSizeKeyName = "cellCache.maxCellSize";

		public static readonly ConfigKey DatabaseName = new ConfigKey(DatabaseNameKeyName, typeof(string));

		public static readonly ConfigKey DefaultSchema = new ConfigKey(DefaultSchemKeyName, "APP", typeof(string));

		public static readonly ConfigKey StorageSystem = new ConfigKey(StoreSystemKeyName, DefaultStorageSystemNames.Heap, typeof(string));

		public static readonly ConfigKey DeleteOnClose = new ConfigKey(DeleteOnCloseKeyName, false, typeof(bool));

		public static readonly ConfigKey QueryPlanner = new ConfigKey(QueryPlannerKeyName, null, typeof(string));

		public static readonly ConfigKey CellCacheType = new ConfigKey(CellCacheTypeKeyName, null, typeof(Type));

		public static readonly ConfigKey CellCacheMaxSize = new ConfigKey(CellCacheMaxSizeKeyName, 1024*1024*10, typeof(int));

		public static readonly ConfigKey CellCacheMaxCellSize = new ConfigKey(CellCacheMaxCellSizeKeyName, 1024*64, typeof(int));
	}
}
