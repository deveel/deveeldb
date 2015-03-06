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

namespace Deveel.Data.Caching {
	public static class CacheConfigKeys {
		public const string CacheTypeKeyName = "caching.type";
		public const string DataCacheSizeKeyName = "caching.dataCacheSize";
		public const string MaxEntrySizeKeyName = "caching.maxEntrySize";
		public const string CacheStatementsKeyName = "caching.cacheStatements";

		public static readonly ConfigKey CacheType = new ConfigKey(CacheTypeKeyName, typeof(MemoryCache).AssemblyQualifiedName, typeof(string));
		public static readonly ConfigKey DataCacheSize = new ConfigKey(DataCacheSizeKeyName, 1024, typeof(int));
		public static readonly ConfigKey MaxCacheEntrySize = new ConfigKey(MaxEntrySizeKeyName, typeof(int));
		public static readonly ConfigKey CacheStatements = new ConfigKey(CacheStatementsKeyName, true, typeof(bool));

	}
}