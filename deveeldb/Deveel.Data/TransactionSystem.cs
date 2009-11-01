//  
//  TransactionSystem.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Data.Control;
using Deveel.Data.Functions;
using Deveel.Data.Store;
using Deveel.Data.Text;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A class that provides information and global functions for the 
	/// transaction layer in the engine. 
	/// </summary>
	/// <remarks>
	/// Shared information includes configuration details, logging, etc.
	/// </remarks>
	public class TransactionSystem : IDisposable {
		/// <summary>
		/// The stats object that keeps track of database statistics.
		/// </summary>
		private readonly Stats stats = new Stats();

		/// <summary>
		/// If this is set to true, during boot up the engine will reindex all the
		/// tables that weren't closed.  If false, the engine will only reindex the
		/// tables that have unchecked in modifications.
		/// </summary>
		private bool always_reindex_dirty_tables = false;

		/// <summary>
		/// A LoggingBufferManager object used to manage pages of ScatteringFileStore
		/// objects in the file system.  We can configure the maximum pages and page
		/// size via this object, so we have control over how much memory from the
		/// heap is used for buffering.
		/// </summary>
		private LoggingBufferManager buffer_manager;

		/// <summary>
		/// The configuration properties of the entire database system.
		/// </summary>
		private IDbConfig config = null;

		/// <summary>
		///  The DataCellCache that is a shared resource between on database's.
		/// </summary>
		private DataCellCache data_cell_cache = null;

		/// <summary>
		/// The path in the file system for the database files. 
		/// </summary>
		/// <remarks>
		/// Note that this will be null if the database does not exist in a local 
		/// file system.  For this reason it's best not to write code that relies 
		/// on the use of this value.
		/// </remarks>
		private string db_path;

		/// <summary>
		/// The dispatcher.
		/// </summary>
		private DatabaseDispatcher dispatcher;

		/// <summary>
		/// Set to true if the file handles should NOT be synchronized with the
		/// system file IO when the indices are written.  If this is true, then the
		/// database is not as fail safe, however File IO performance is improved.
		/// </summary>
		private bool dont_synch_filesystem = false;

		/// <summary>
		///  The list of FunctionFactory objects that handle different functions from SQL.
		/// </summary>
		private ArrayList function_factory_list;

		/// <summary>
		/// The IFunctionLookup object that can resolve a FunctionDef object to a
		/// IFunction object.
		/// </summary>
		private DSFunctionLookup function_lookup;

		/// <summary>
		/// Set to true if the parser should ignore case when searching for a schema,
		/// table or column using an identifier.
		/// </summary>
		private bool ignore_case_for_identifiers = false;

		/// <summary>
		/// The log directory.
		/// </summary>
		private string log_directory;

		/// <summary>
		/// Set to true if lookup comparison lists are enabled.
		/// </summary>
		private bool lookup_comparison_list_enabled = false;

		/// <summary>
		/// Set to true if the database is in Read only mode.  This is set from the
		/// configuration file.
		/// </summary>
		private bool read_only_access = false;

		/// <summary>
		/// The regular expression library bridge for the library we are configured
		/// to use.
		/// </summary>
		private IRegexLibrary regex_library;

		/// <summary>
		/// Set to false if there is conservative index memory storage.  If true,
		/// all root selectable schemes are stored behind a soft reference that will
		/// be garbage collected.
		/// </summary>
		private bool soft_index_storage = false;

		/// <summary>
		/// The underlying IStoreSystem implementation that encapsulates the behaviour
		/// for storing data persistantly.
		/// </summary>
		private IStoreSystem store_system;

		/// <summary>
		/// A logger to output any debugging messages.
		/// </summary>
		private IDebugLogger logger;

		// ---------- Low level row listeners ----------

		/// <summary>
		/// A list of table names and listeners that are notified of add and remove
		/// events in a table.
		/// </summary>
		private ArrayList table_listeners;

		/// <summary>
		/// Set to true if locking checks should be performed each time a table is 
		/// accessed.
		/// </summary>
		private bool table_lock_check = false;

		/// <summary>
		/// Transaction option, if this is true then a transaction error is generated
		/// during commit if a transaction selects data from a table that has
		/// committed changes to it during commit time.
		/// </summary>
		private bool transaction_error_on_dirty_select = true;


		///<summary>
		///</summary>
		public TransactionSystem() {
			// Setup generate properties from the environment.
			stats.Set(0, "OS.Platform: " + Environment.OSVersion.Platform);
			stats.Set(0, "OS.Version: " + Environment.OSVersion.VersionString);
			stats.Set(0, "Runtime.Version: " + Environment.Version);
			stats.Set(0, "Machine.ProcessorCount: " + Environment.ProcessorCount);
			table_listeners = new ArrayList();
		}

		~TransactionSystem() {
			Dispose(false);
		}

		/// <summary>
		/// Returns true if the database is in read only mode.
		/// </summary>
		/// <remarks>
		/// In read only mode, any 'write' operations are not permitted.
		/// </remarks>
		public bool ReadOnlyAccess {
			get { return read_only_access; }
		}

		/// <summary>
		/// Returns the path of the database in the local file system if the database
		/// exists within the local file system.
		/// </summary>
		/// <remarks>
		/// If the database is not within the local file system then null is returned. 
		/// It is recommended this method is not used unless for legacy or compatability 
		/// purposes.
		/// </remarks>
		public string DatabasePath {
			get { return db_path; }
		}

		/// <summary>
		/// Returns true if the database should perform checking of table locks.
		/// </summary>
		public bool TableLockingEnabled {
			get { return table_lock_check; }
		}

		/// <summary>
		/// Returns true if we should generate lookup caches in InsertSearch otherwise
		/// returns false.
		/// </summary>
		public bool LookupComparisonListEnabled {
			get { return lookup_comparison_list_enabled; }
		}

		///<summary>
		/// Returns true if all table indices are kept behind a soft reference that
		/// can be garbage collected.
		///</summary>
		public bool SoftIndexStorage {
			get { return soft_index_storage; }
		}

		///<summary>
		/// Returns the status of the 'always_reindex_dirty_tables' property.
		///</summary>
		public bool AlwaysReindexDirtyTables {
			get { return always_reindex_dirty_tables; }
		}

		///<summary>
		///  Returns true if we shouldn't synchronize with the file system when
		/// important indexing information is flushed to the disk.
		///</summary>
		public bool DontSynchFileSystem {
			get { return dont_synch_filesystem; }
		}

		///<summary>
		/// Returns true if during commit the engine should look for any selects
		/// on a modified table and fail if they are detected.
		///</summary>
		public bool TransactionErrorOnDirtySelect {
			get { return transaction_error_on_dirty_select; }
		}

		///<summary>
		/// Returns true if the parser should ignore case when searching for
		/// schema/table/column identifiers.
		///</summary>
		public bool IgnoreIdentifierCase {
			get { return ignore_case_for_identifiers; }
		}

		///<summary>
		/// Returns the LoggingBufferManager object enabling us to create no file
		/// stores in the file system.
		///</summary>
		/// <remarks>
		/// This provides access to the buffer scheme that has been configured.
		/// </remarks>
		public LoggingBufferManager BufferManager {
			get { return buffer_manager; }
		}

		///<summary>
		/// Returns the regular expression library from the configuration file.
		///</summary>
		///<exception cref="ApplicationException"></exception>
		public IRegexLibrary RegexLibrary {
			get {
				if (regex_library != null) {
					return regex_library;
				}
				throw new ApplicationException("No regular expression library found in classpath " +
				                               "and/or in configuration file.");
			}
		}

		/// <summary>
		/// Gets an instance to the configurations set to the database system.
		/// </summary>
		internal IDbConfig Config {
			get { return config; }
		}

		// ---------- Store System encapsulation ----------

		/// <summary>
		/// Returns the IStoreSystem encapsulation being used in this database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return store_system; }
		}

		// ---------- Debug logger methods ----------

		///<summary>
		/// Returns the IDebugLogger object that is used to log debug message. 
		///</summary>
		/// <remarks>
		/// This property must always return a debug logger that we can log to.
		/// </remarks>
		public IDebugLogger Debug {
			get { return logger; }
		}

		// ---------- Function factories ----------

		///<summary>
		/// Returns a <see cref="IFunctionLookup"/> object that will search through the 
		/// function factories in this database system and find and resolve a function.
		///</summary>
		/// <remarks>
		/// The returned object may throw an exception from the <see cref="FunctionDef.GetFunction"/> 
		/// method if the <see cref="FunctionDef"/> is invalid. For example, if the number 
		/// of parameters is incorrect or the name can not be found.
		/// </remarks>
		public IFunctionLookup FunctionLookup {
			get { return function_lookup; }
		}

		// ---------- System preparers ----------

		// ---------- Database System Statistics Methods ----------

		/// <summary>
		/// Returns a <see cref="Stats"/> object that can be used to keep 
		/// track of database statistics for this environment.
		/// </summary>
		public Stats Stats {
			get { return stats; }
		}

		// ---------- Log directory management ----------

		/// <summary>
		/// Gets or sets the current log directory or null if no logging 
		/// should occur.
		/// </summary>
		/// <remarks>
		/// Setting this should preferably be called during initialization.
		/// </remarks>
		public string LogDirectory {
			get { return log_directory; }
			set { log_directory = value; }
		}

		// ---------- Cache Methods ----------

		/// <summary>
		/// Returns a <see cref="DataCellCache"/> object that is a shared 
		/// resource between all database's running on this runtime.
		/// </summary>
		internal DataCellCache DataCellCache {
			get { return data_cell_cache; }
		}

		// ---------- Dispatch methods ----------

		/// <summary>
		/// Returns the <see cref="DatabaseDispatcher"/> object.
		/// </summary>
		private DatabaseDispatcher Dispatcher {
			get {
				lock (this) {
					if (dispatcher == null) {
						dispatcher = new DatabaseDispatcher(this);
					}
					return dispatcher;
				}
			}
		}

		/// <summary>
		/// Parses a file string to an absolute position in the file system.
		/// </summary>
		/// <remarks>
		/// We must provide the path to the root directory (eg. the directory 
		/// where the config bundle is located).
		/// </remarks>
		internal static string ParseFileString(string root_path, String root_info, String path_string) {
			string path = Path.GetFullPath(path_string);
			string res;
			// If the path is absolute then return the absoluate reference
			if (Path.IsPathRooted(path_string)) {
				res = path;
			} else {
				// If the root path source is the environment then just return the path.
				if (root_info != null &&
				    root_info.Equals("env")) {
					return path;
				}
					// If the root path source is the configuration file then
					// concat the configuration path with the path string and return it.
				else {
					res = Path.Combine(root_path, path_string);
				}
			}
			return res;
		}

		private void SetupFSync(IDbConfig dbConfig) {
			string fsyncTypeString = dbConfig.GetValue("fsync_type");
			if (fsyncTypeString != null) {
				// if the 'fsync_type' is set and the value is "default" we use 
				// the one retrieved automatically at the call of FSync class...
				if (String.Compare(fsyncTypeString, "default", true) == 0)
					return;

				Type type = Type.GetType(fsyncTypeString, false, true);
				if (type == null) {
					// there is no custom implementation of the fsync() operation.
					Debug.Write(DebugLevel.Warning, this,
					            "The value of 'fsync_type' is set but the type '" + fsyncTypeString + "' was not found.");
					return;
				}

				IFSync fsync = null;

				if (typeof(IFSync).IsAssignableFrom(type)) {
					try {
						fsync = (IFSync) Activator.CreateInstance(type, true);
					} catch (Exception e) {
						Debug.Write(DebugLevel.Warning, this,
						            "Error while initializing the fsynch handler class '" + fsyncTypeString + "': " + e.Message);
					}
				} else {
					// handle the case the type implements the Sync function but
					// doesn't implements the IFSync interface...
					try {
						fsync = FSync.Create(type);
					} catch (Exception) {
						Debug.Write(DebugLevel.Warning, this, "The provided type '" + type.FullName + "' does not implement the Sync(FileStream) method.");
					}
				}

				if (fsync != null)
					FSync.SetFSync(fsync);
			}
		}

		/// <summary>
		/// Sets up the log file from the config information.
		/// </summary>
		/// <param name="config"></param>
		private void SetupLog(IDbConfig config) {
			//// Conditions for not initializing a log directory;
			////  1. Read only access is enabled
			////  2. log_path is empty or not set

			string log_path_string = config.GetValue("log_path");
			string debug_logs = config.GetValue("debug_logs");
			string read_only = config.GetValue("read_only");
			string root_path_var = config.GetValue("root_path");

			bool read_only_bool = false;
			if (read_only != null)
				read_only_bool = String.Compare(read_only, "enabled", true) == 0;
			bool debug_logs_bool = true;
			if (debug_logs != null)
				debug_logs_bool = String.Compare(debug_logs, "enabled", true) == 0;

			if (debug_logs_bool && !read_only_bool &&
				log_path_string != null && !log_path_string.Equals("")) {
				// First set up the debug information in this VM for the 'Debug' class.
				string log_path = ParseFileString(config.CurrentPath, root_path_var, log_path_string);
				// If the path doesn't exist the make it.
				if (!Directory.Exists(log_path))
					Directory.CreateDirectory(log_path);
				
				LogDirectory = log_path;
			}

			string logger_type_string = config.GetValue("logger_type");

			Type logger_type = null;
			if (logger_type_string != null) {
				logger_type = Type.GetType(logger_type_string, false, true);
				if (!typeof(IDebugLogger).IsAssignableFrom(logger_type))
					logger_type = null;
			}

			// in case we don't log...
			if (read_only_bool || !debug_logs_bool)
				logger_type = typeof (EmptyDebugLogger);

			if (logger_type == null)
				logger_type = typeof (DefaultDebugLogger);

			logger = (IDebugLogger) Activator.CreateInstance(logger_type, true);
			logger.Init(config);
		}

		/// <summary>
		/// Returns a configuration value, or the default if it's not found.
		/// </summary>
		/// <param name="property"></param>
		/// <param name="default_val"></param>
		/// <returns></returns>
		public String GetConfigString(String property, String default_val) {
			String v = config.GetValue(property);
			if (v == null) {
				return default_val;
			}
			return v.Trim();
		}

		/// <summary>
		/// Returns a configuration value, or the default if it's not found.
		/// </summary>
		/// <param name="property"></param>
		/// <param name="default_val"></param>
		/// <returns></returns>
		public int GetConfigInt(String property, int default_val) {
			String v = config.GetValue(property);
			if (v == null) {
				return default_val;
			}
			return Int32.Parse(v);
		}

		/// <summary>
		/// Returns a configuration value, or the default if it's not found.
		/// </summary>
		/// <param name="property"></param>
		/// <param name="default_val"></param>
		/// <returns></returns>
		public bool GetConfigBoolean(String property, bool default_val) {
			String v = config.GetValue(property);
			if (v == null) {
				return default_val;
			}
			return String.Compare(v.Trim(), "enabled", true) == 0;
		}


		/// <summary>
		/// Given a regular expression string representing a particular library, this
		/// will return the name of the type to use as a bridge between the library
		/// and the database engine.
		/// </summary>
		/// <returns>
		/// Returns null if the library name is invalid.
		/// </returns>
		private static String RegexStringToClass(String lib) {
			if (lib.Equals("System.RegEx")) {
				return "Deveel.Data.Text.SystemRegexLibrary";
			} else if (lib.Equals("Apache.RegEx")) {
				return "Deveel.Data.Text.ApacheRegexLibrary";
			} else if (lib.Equals("Gnu.RegEx")) {
				return "Deveel.Data.Text.GNURegexLibrary";
			} else {
				return null;
			}
		}

		/// <summary>
		/// Inits the <see cref="TransactionSystem"/> with the configuration 
		/// properties of the system.
		/// </summary>
		/// <param name="config"></param>
		/// <remarks>
		/// This can only be called once, and should be called at database boot time.
		/// </remarks>
		public virtual void Init(IDbConfig config) {
			function_factory_list = new ArrayList();
			function_lookup = new DSFunctionLookup();

			if (config != null) {
				this.config = config;

				// Set the read_only property
				read_only_access = GetConfigBoolean("read_only", false);

				// Setup the log
				SetupLog(config);

				// The storage encapsulation that has been configured.
				String storage_system = GetConfigString("storage_system", "v1file");

				bool is_file_store_mode;

				// Construct the system store.
				if (String.Compare(storage_system, "v1file", true) == 0) {
					Debug.Write(DebugLevel.Message, this, "Storage System: v1 file storage mode.");

					// Check if the configuration provides a custom implementation of fsync()
					SetupFSync(config);

					// The path where the database data files are stored.
					String database_path = GetConfigString("database_path", "./data");
					// The root path variable
					String root_path_var = GetConfigString("root_path", null);

					// Set the absolute database path
					db_path = ParseFileString(config.CurrentPath, root_path_var, database_path);

					store_system = new V1FileStoreSystem(this, db_path, read_only_access);
					is_file_store_mode = true;
				} else if (String.Compare(storage_system, "v1heap", true) == 0) {
					Debug.Write(DebugLevel.Message, this, "Storage System: v1 heap storage mode.");
					store_system = new V1HeapStoreSystem();
					is_file_store_mode = false;
				} else {
					String error_msg = "Unknown storage_system property: " + storage_system;
					Debug.Write(DebugLevel.Error, this, error_msg);
					throw new Exception(error_msg);
				}

				// Register the internal function factory,
				AddFunctionFactory(new InternalFunctionFactory());

				// Set up the DataCellCache from the values in the configuration
				int max_cache_size = 0, max_cache_entry_size = 0;

				max_cache_size = GetConfigInt("data_cache_size", 0);
				max_cache_entry_size = GetConfigInt("max_cache_entry_size", 0);

				if (max_cache_size >= 4096 &&
				    max_cache_entry_size >= 16 &&
				    max_cache_entry_size < (max_cache_size/2)) {
					Debug.Write(DebugLevel.Message, this,"Internal Data Cache size:          " + max_cache_size);
					Debug.Write(DebugLevel.Message, this,"Internal Data Cache max cell size: " + max_cache_entry_size);

					// Find a prime hash size depending on the size of the cache.
					int hash_size = DataCellCache.ClosestPrime(max_cache_size/55);

					// Set up the data_cell_cache
					data_cell_cache = new DataCellCache(this, max_cache_size, max_cache_entry_size, hash_size);
				} else {
					Debug.Write(DebugLevel.Message, this, "Internal Data Cache disabled.");
				}

				// Are lookup comparison lists enabled?
				//      lookup_comparison_list_enabled =
				//                            GetConfigBoolean("lookup_comparison_list", false);
				lookup_comparison_list_enabled = false;
				Debug.Write(DebugLevel.Message, this, "lookup_comparison_list = " + lookup_comparison_list_enabled);

				// Should we open the database in Read only mode?
				Debug.Write(DebugLevel.Message, this, "read_only = " + read_only_access);
				if (read_only_access) stats.Set(1, "DatabaseSystem.read_only");

				//      // Hard Sync file system whenever we update index files?
				//      if (is_file_store_mode) {
				//        dont_synch_filesystem = GetConfigBoolean("dont_synch_filesystem", false);
				//        Debug.Write(DebugLevel.Message, this,
				//                      "dont_synch_filesystem = " + dont_synch_filesystem);
				//      }

				// Generate transaction error if dirty selects are detected?
				transaction_error_on_dirty_select = GetConfigBoolean("transaction_error_on_dirty_select", true);
				Debug.Write(DebugLevel.Message, this, "transaction_error_on_dirty_select = " + transaction_error_on_dirty_select);

				// Case insensitive identifiers?
				ignore_case_for_identifiers = GetConfigBoolean("ignore_case_for_identifiers", false);
				Debug.Write(DebugLevel.Message, this, "ignore_case_for_identifiers = " + ignore_case_for_identifiers);

				// ---- Store system setup ----

				if (is_file_store_mode) {
					// Get the safety level of the file system where 10 is the most safe
					// and 1 is the least safe.
					int io_safety_level = GetConfigInt("io_safety_level", 10);
					if (io_safety_level < 1 || io_safety_level > 10) {
						Debug.Write(DebugLevel.Message, this, "Invalid io_safety_level value.  Setting to the most safe level.");
						io_safety_level = 10;
					}
					Debug.Write(DebugLevel.Message, this, "io_safety_level = " + io_safety_level);

					// Logging is disabled when safety level is less or equal to 2
					bool enable_logging = true;
					if (io_safety_level <= 2) {
						Debug.Write(DebugLevel.Message, this, "Disabling journaling and file sync.");
						enable_logging = false;
					}

					Debug.Write(DebugLevel.Message, this, "Using stardard IO API for heap buffered file access.");
					int page_size = GetConfigInt("buffered_io_page_size", 8192);
					int max_pages = GetConfigInt("buffered_io_max_pages", 256);

					// Output this information to the log
					Debug.Write(DebugLevel.Message, this, "[Buffer Manager] Page Size: " + page_size);
					Debug.Write(DebugLevel.Message, this, "[Buffer Manager] Max pages: " + max_pages);

					// Journal path is currently always the same as database path.
					string journal_path = db_path;
					// Max slice size is 1 GB for file scattering class
					const long max_slice_size = 16384*65536;
					// First file extention is 'db'
					const String first_file_ext = "db";

					// Set up the BufferManager
					buffer_manager = new LoggingBufferManager(
						db_path, journal_path, read_only_access, max_pages, page_size,
						first_file_ext, max_slice_size, Debug, enable_logging);
					// ^ This is a big constructor.  It sets up the logging manager and
					//   sets a resource store data accessor converter to a scattering
					//   implementation with a max slice size of 1 GB

					// Start the buffer manager.
					try {
						buffer_manager.Start();
					} catch (IOException e) {
						Debug.Write(DebugLevel.Error, this, "Error starting buffer manager");
						Debug.WriteException(DebugLevel.Error, e);
						throw new ApplicationException("IO Error: " + e.Message);
					}
				}

				// What regular expression library are we using?
				// If we want the engine to support other regular expression libraries
				// then include the additional entries here.

				String regex_bridge;
				String lib_used;

				String force_lib = GetConfigString("force_regex_library", null);

				// Are we forcing a particular regular expression library?
				if (force_lib != null) {
					lib_used = force_lib;
					// Convert the library string to a class name
					regex_bridge = RegexStringToClass(force_lib);
				} else {
					String lib = GetConfigString("regex_library", "Deveel.Data.Text.DeveelRegexLibrary");
					lib_used = lib;
					// Convert the library string to a class name
					regex_bridge = lib != null ? RegexStringToClass(lib) : "Deveel.Data.Text.SystemRegexLibrary";
				}

				if (regex_bridge != null) {
					try {
						Type c = Type.GetType(regex_bridge);
						regex_library = (IRegexLibrary) Activator.CreateInstance(c);
						Debug.Write(DebugLevel.Message, this, "Using regex bridge: " + lib_used);
					} catch (Exception e) {
						Debug.Write(DebugLevel.Error, this, "Unable to load regex bridge: " + regex_bridge);
						Debug.WriteException(DebugLevel.Warning, e);
					}
				} else {
					if (lib_used != null)
						Debug.Write(DebugLevel.Error, this, "Regex library not known: " + lib_used);
					Debug.Write(DebugLevel.Message, this, "Regex features disabled.");
				}

				// ---------- Plug ins ---------

				try {
					// The 'function_factories' property.
					String function_factories =
						GetConfigString("function_factories", null);
					if (function_factories != null) {
						string[] factories = function_factories.Split(';');
						for (int i = 0; i < factories.Length; ++i) {
							string factory_class = factories[i];
							Type c = Type.GetType(factory_class);
							FunctionFactory fun_factory = (FunctionFactory) Activator.CreateInstance(c);
							AddFunctionFactory(fun_factory);
							Debug.Write(DebugLevel.Message, this, "Successfully added function factory: " + factory_class);
						}
					} else {
						Debug.Write(DebugLevel.Message, this, "No 'function_factories' config property found.");
						// If resource missing, do nothing...
					}
				} catch (Exception e) {
					Debug.Write(DebugLevel.Error, this, "Error parsing 'function_factories' configuration property.");
					Debug.WriteException(e);
				}

				// Flush the contents of the function lookup object.
				FlushCachedFunctionLookup();
			}
		}

		/**
		 * Hack - set up the DataCellCache in DatabaseSystem so we can use the
		 * MasterTableDataSource object without having to boot a new DatabaseSystem.
		 */

		public void SetupRowCache(int max_cache_size, int max_cache_entry_size) {
			// Set up the data_cell_cache
			data_cell_cache =
				new DataCellCache(this, max_cache_size, max_cache_entry_size);
		}

		///<summary>
		/// Registers a new FunctionFactory with the database system.
		///</summary>
		///<param name="factory"></param>
		/// <remarks>
		/// The function factories are used to resolve a function name into a IFunction object.
		/// Function factories are checked in the order they are added to the database system.
		/// </remarks>
		public void AddFunctionFactory(FunctionFactory factory) {
			lock (function_factory_list) {
				function_factory_list.Add(factory);
			}
			factory.Init();
		}

		///<summary>
		/// Flushes the <see cref="IFunctionLookup"/> object returned by the 
		/// <see cref="FunctionLookup"/> property.
		///</summary>
		/// <remarks>
		/// This should be called if the function factory list has been modified 
		/// in some way.
		/// </remarks>
		public void FlushCachedFunctionLookup() {
			FunctionFactory[] factories;
			lock (function_factory_list) {
				factories = (FunctionFactory[]) function_factory_list.ToArray(typeof (FunctionFactory));
			}
			function_lookup.flushContents(factories);
		}

		///<summary>
		/// Given a <see cref="Transaction.CheckExpression"/>, this will prepare 
		/// the expression and return a new prepared <see cref="Transaction.CheckExpression"/>.
		///</summary>
		///<param name="table_def"></param>
		///<param name="check"></param>
		/// <remarks>
		/// The default implementation of this is to do nothing.  However, a sub-class 
		/// of the system choose to prepare the expression, such as resolving the 
		/// functions via the function lookup, and resolving the sub-queries, etc.
		/// </remarks>
		///<returns></returns>
		public virtual Transaction.CheckExpression PrepareTransactionCheckConstraint(
			DataTableDef table_def, Transaction.CheckExpression check) {
			//    IExpressionPreparer expression_preparer = getFunctionExpressionPreparer();
			// Resolve the expression to this table and row and evaluate the
			// check constraint.
			Expression exp = check.expression;
			table_def.ResolveColumns(IgnoreIdentifierCase, exp);
			//    try {
			//      // Prepare the functions
			//      exp.Prepare(expression_preparer);
			//    }
			//    catch (Exception e) {
			//      Debug.WriteException(e);
			//      throw new ApplicationException(e.Message);
			//    }

			return check;
		}


		/// <summary>
		///  Creates an event object that is passed into <see cref="PostEvent"/> 
		/// method to run the given <see cref="EventHandler"/> method after the 
		/// time has passed.
		/// </summary>
		/// <param name="runnable"></param>
		/// <remarks>
		/// The event created here can be safely posted on the event queue as many
		/// times as you like.  It's useful to create an event as a persistant object
		/// to service some event.  Just post it on the dispatcher when you want
		/// it run!
		/// </remarks>
		/// <returns></returns>
		internal Object CreateEvent(IDatabaseEvent runnable) {
			return Dispatcher.CreateEvent(runnable);
		}

		/// <summary>
		/// Adds a new event to be dispatched on the queue after 'time_to_wait'
		/// milliseconds has passed.
		/// </summary>
		/// <param name="time_to_wait"></param>
		/// <param name="e">An event object returned by <see cref="CreateEvent"/>.</param>
		internal void PostEvent(int time_to_wait, Object e) {
			Dispatcher.PostEvent(time_to_wait, e);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				GC.SuppressFinalize(this);
				Dispose();
			}
		}

		public virtual void Dispose() {
			if (buffer_manager != null) {
				try {
					// Set a check point
					store_system.SetCheckPoint();
					// Stop the buffer manager
					buffer_manager.Stop();
				} catch (IOException e) {
					Console.Out.WriteLine("Error stopping buffer manager.");
					Console.Out.Write(e.StackTrace);
				}
			}
			buffer_manager = null;
			regex_library = null;
			data_cell_cache = null;
			config = null;
			log_directory = null;
			function_factory_list = null;
			store_system = null;
			if (dispatcher != null) {
				dispatcher.Finish();
			}
			//    trigger_manager = null;
			dispatcher = null;
			Debug.Dispose();
		}


		// ---------- Inner classes ----------

		#region Nested type: DSFunctionLookup

		/// <summary>
		/// A <see cref="IFunctionLookup"/> implementation that will look up a 
		/// function from a list of <see cref="FunctionFactory"/> objects 
		/// provided with.
		/// </summary>
		private sealed class DSFunctionLookup : IFunctionLookup {
			private FunctionFactory[] factories;

			#region IFunctionLookup Members

			public IFunction GenerateFunction(FunctionDef function_def) {
				lock (this) {
					for (int i = 0; i < factories.Length; ++i) {
						IFunction f = factories[i].GenerateFunction(function_def);
						if (f != null) {
							return f;
						}
					}
					return null;
				}
			}

			public bool IsAggregate(FunctionDef function_def) {
				lock (this) {
					for (int i = 0; i < factories.Length; ++i) {
						IFunctionInfo f_info =
							factories[i].GetFunctionInfo(function_def.Name);
						if (f_info != null) {
							return f_info.Type == FunctionType.Aggregate;
						}
					}
					return false;
				}
			}

			#endregion

			public void flushContents(FunctionFactory[] factories) {
				lock (this) {
					this.factories = factories;
				}
			}
		}

		#endregion
	}
}