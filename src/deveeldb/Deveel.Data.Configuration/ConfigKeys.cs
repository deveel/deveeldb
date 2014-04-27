// 
//  Copyright 2010-2013  Deveel
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

namespace Deveel.Data.Configuration {
	/// <summary>
	/// This static class exposes the well-known configuration keys
	/// available to setup the system.
	/// </summary>
	/// <remarks>
	/// The keys exposed by the class are not an enumeration
	/// of all the possible configurations: actually, developers
	/// of modules to the kernel could define their own configurations
	/// that are fully legitimate.
	/// <para>
	/// The aim of this class is to provide the system with a
	/// minimum set of configuration keys to provide the basic
	/// setup of the system.
	/// </para>
	/// </remarks>
	/// <seealso cref="DbConfig"/>
	/// <seealso cref="ConfigDefaultValues"/>
	public static class ConfigKeys {
		/// <summary>
		/// Defines the base for resolving the application
		/// other paths defined by the environment.
		/// </summary>
		public const string BasePath = "base_path";
		
		public const string DatabasePath = "database_path";

		public const string LogPath = "log_path";

		public const string IgnoreIdentifiersCase = "ignore_case_for_identifiers";

		public const string RegexLibrary = "regex_library";

		/// <summary>
		/// The key to configure the type of the system used to store
		/// database data.
		/// </summary>
		/// <seealso cref="ConfigDefaultValues.FileStorageSystem"/>
		/// <seealso cref="ConfigDefaultValues.HeapStorageSystem"/>
		public const string StorageSystem = "storage_system";

		public const string DataCacheSize = "data_cache_size";

		public const string MaxCacheEntrySize = "max_cache_entry_size";

		public const string LookupComparisonList = "lookup_comparison_list";

		public const string MaxWorkerThreads = "maximum_worker_threads";

		public const string TransactionErrorOnDirtySelect = "transaction_error_on_dirty_select";

		public const string ReadOnly = "read_only";

		public const string DebugLogFile = "debug_log_file";

		public const string DebugLevel = "debug_level";

		public const string DebugLogs = "debug_logs";

		public const string TableLockCheck = "table_lock_check";

		public const string CacheStatements = "statements_cache";

		public const string LogQueries = "query_logging";

		public const string LoggerType = "logger_type";

		public const string CacheType = "cache_type";

		public const string ForceRegexLibrary = "force_regex_library";

		public const string PasswordHashFunction = "password_hash_function";

		public const string DefaultSchema = "default_schema";
	}
}