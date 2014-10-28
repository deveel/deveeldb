// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.DbSystem {
	public static class SystemConfigKeys {
		public const string DatabaseNameKeyName = "system.databaseName";
		public const string IgnoreCaseKeyName = "system.ignoreCase";
		public const string MaxWorkerThreadsKeyName = "system.maxWorkerThreads";
		public const string ReadOnlyKeyName = "system.readOnly";
		public const string DefaultSchemKeyName = "system.defaultSchema";

		public static readonly ConfigKey DatabaseName = new ConfigKey(DatabaseNameKeyName, typeof(string));
		public static readonly ConfigKey IgnoreCase = new ConfigKey(IgnoreCaseKeyName, false, typeof (bool));
		public static readonly ConfigKey MaxWorkerThreads = new ConfigKey(MaxWorkerThreadsKeyName, 5, typeof(int));
		public static readonly ConfigKey ReadOnly = new ConfigKey(ReadOnlyKeyName, false, typeof(bool));
		public static readonly ConfigKey DefaultSchema = new ConfigKey(DefaultSchemKeyName, "APP", typeof(string));

		internal static void SetTo(IDbConfig config) {
			config.SetKey(DatabaseName);
			config.SetKey(IgnoreCase);
			config.SetKey(MaxWorkerThreads);
			config.SetKey(ReadOnly);
			config.SetKey(DefaultSchema);
		}
	}
}