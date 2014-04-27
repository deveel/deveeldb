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
using System.Collections;
using System.IO;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.DbSystem;
using Deveel.Data.Store;
using Deveel.Data.Text;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A class that provides information and global functions for the 
	/// transaction layer in the engine. 
	/// </summary>
	/// <remarks>
	/// Shared information includes configuration details, logging, etc.
	/// </remarks>
	public class SystemContext : ISystemContext {
		/// <summary>
		/// The dispatcher.
		/// </summary>
		private DatabaseDispatcher dispatcher;

		/// <summary>
		///  The list of FunctionFactory objects that handle different functions from SQL.
		/// </summary>
		private ArrayList functionFactoryList;

		/// <summary>
		/// The regular expression library bridge for the library we are configured
		/// to use.
		/// </summary>
		private IRegexLibrary regexLibrary;

		// private TypesManager typesManager;


		// ---------- Low level row listeners ----------

		/// <summary>
		/// Set to true if locking checks should be performed each time a table is 
		/// accessed.
		/// </summary>
		private bool tableLockCheck = false;

		///<summary>
		///</summary>
		public SystemContext() {
			Stats = new Stats();
			TransactionErrorOnDirtySelect = true;
			// Setup generate properties from the environment.
			Stats.Set("OS.Platform: " + Environment.OSVersion.Platform, 0);
			Stats.Set("OS.Version: " + Environment.OSVersion.VersionString, 0);
			Stats.Set("Runtime.Version: " + Environment.Version, 0);
			Stats.Set("Machine.ProcessorCount: " + Environment.ProcessorCount, 0);
		}

		~SystemContext() {
			Dispose(false);
		}

		/// <summary>
		/// Returns true if the database is in read only mode.
		/// </summary>
		/// <remarks>
		/// In read only mode, any 'write' operations are not permitted.
		/// </remarks>
		public bool ReadOnlyAccess { get; private set; }

		/// <summary>
		/// Returns the path of the database in the local file system if the database
		/// exists within the local file system.
		/// </summary>
		/// <remarks>
		/// If the database is not within the local file system then null is returned. 
		/// It is recommended this method is not used unless for legacy or compatability 
		/// purposes.
		/// </remarks>
		public string DatabasePath { get; private set; }

		/// <summary>
		/// Returns true if the database should perform checking of table locks.
		/// </summary>
		public bool TableLockingEnabled {
			get { return tableLockCheck; }
		}

		///<summary>
		/// Returns true if during commit the engine should look for any selects
		/// on a modified table and fail if they are detected.
		///</summary>
		public bool TransactionErrorOnDirtySelect { get; private set; }

		///<summary>
		/// Returns true if the parser should ignore case when searching for
		/// schema/table/column identifiers.
		///</summary>
		public bool IgnoreIdentifierCase { get; private set; }

		///<summary>
		/// Returns the regular expression library from the configuration file.
		///</summary>
		///<exception cref="ApplicationException"></exception>
		public IRegexLibrary RegexLibrary {
			get {
				if (regexLibrary != null) {
					return regexLibrary;
				}
				throw new ApplicationException("No regular expression library found in classpath " +
				                               "and/or in configuration file.");
			}
		}

		/// <summary>
		/// Gets an instance to the configurations set to the database system.
		/// </summary>
		internal IDbConfig Config { get; private set; }

		IDbConfig ISystemContext.Config {
			get { return Config; }
		}

		// ---------- Store System encapsulation ----------

		/// <summary>
		/// Returns the IStoreSystem encapsulation being used in this database.
		/// </summary>
		public IStoreSystem StoreSystem { get; private set; }

		// ---------- Logger logger methods ----------

		///<summary>
		/// Returns the ILogger object that is used to log debug message. 
		///</summary>
		/// <remarks>
		/// This property must always return a debug logger that we can log to.
		/// </remarks>
		public ILogger Logger { get; private set; }

		ILogger ISystemContext.Logger {
			get { return Logger; }
		}

		// ---------- Function factories ----------

		public IRoutineResolver RoutineResolver { get; private set; }

		/*
		public TypesManager TypesManager {
			get { return typesManager; }
		}
		*/

		// ---------- System preparers ----------

		// ---------- Database System Statistics Methods ----------

		/// <summary>
		/// Returns a <see cref="Stats"/> object that can be used to keep 
		/// track of database statistics for this environment.
		/// </summary>
		public Stats Stats { get; private set; }

		// ---------- Log directory management ----------

		/// <summary>
		/// Gets or sets the current log directory or null if no logging 
		/// should occur.
		/// </summary>
		/// <remarks>
		/// Setting this should preferably be called during initialization.
		/// </remarks>
		public string LogDirectory { get; set; }

		// ---------- Cache Methods ----------

		/// <summary>
		/// Returns a <see cref="DataCellCache"/> object that is a shared 
		/// resource between all database's running on this runtime.
		/// </summary>
		public DataCellCache DataCellCache { get; private set; }

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
		/// Sets up the log file from the config information.
		/// </summary>
		private void SetupLog() {
			//// Conditions for not initializing a log directory;
			////  1. Read only access is enabled
			////  2. log_path is empty or not set

			string logPathString = Config.LogPath();
			string rootPathVar = Config.GetValue<string>("root_path");

			bool readOnly = Config.ReadOnly();
			bool debugLogs = Config.GetBoolean(ConfigKeys.DebugLogs, true);

			if (debugLogs && !readOnly && !String.IsNullOrEmpty(logPathString)) {
				// First set up the debug information in this VM for the 'Logger' class.
				string logPath = Config.ParseFileString(rootPathVar, logPathString);
				// If the path doesn't exist the make it.
				if (!Directory.Exists(logPath))
					Directory.CreateDirectory(logPath);
				
				LogDirectory = logPath;
			}

			string loggerTypeString = Config.GetValue<string>(ConfigKeys.LoggerType);

			Type loggerType = null;
			if (loggerTypeString != null) {
				loggerType = Type.GetType(loggerTypeString, false, true);
				if (!typeof(ILogger).IsAssignableFrom(loggerType))
					loggerType = null;
			}

			// in case we don't log...
			if (readOnly || !debugLogs)
				loggerType = typeof (EmptyLogger);

			if (loggerType == null)
				loggerType = typeof (DefaultLogger);

			Logger = (ILogger) Activator.CreateInstance(loggerType, true);
			Logger.Init(Config);
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
			if (lib.Equals("System.RegEx"))
				return "Deveel.Data.Text.SystemRegexLibrary";
			if (lib.Equals("Apache.RegEx"))
				return "Deveel.Data.Text.ApacheRegexLibrary";
			if (lib.Equals("Gnu.RegEx"))
				return "Deveel.Data.Text.GNURegexLibrary";
			return null;
		}

		private void SetupStoreSystem() {
			// The storage encapsulation that has been configured.
			string storageSystem = Config.GetString(ConfigKeys.StorageSystem, ConfigDefaultValues.FileStorageSystem);

			// Construct the system store.
			if (String.Equals(storageSystem, ConfigDefaultValues.FileStorageSystem, StringComparison.InvariantCultureIgnoreCase)) {
				Logger.Message(this, "Storage System: file storage mode.");
				StoreSystem = new V1FileStoreSystem();
			} else if (String.Equals(storageSystem, ConfigDefaultValues.HeapStorageSystem, StringComparison.InvariantCultureIgnoreCase)) {
				Logger.Message(this, "Storage System: heap storage mode.");
				StoreSystem = new V1HeapStoreSystem();
			} else {
				string errorMsg = "Unknown storage_system property: " + storageSystem;

				Type storageSystemType = Type.GetType(storageSystem, false, true);
				if (storageSystemType == null ||
					!typeof(IStoreSystem).IsAssignableFrom(storageSystemType)) {
					Logger.Error(this, errorMsg);
					throw new Exception(errorMsg);
				}

				try {
					StoreSystem = (IStoreSystem)Activator.CreateInstance(storageSystemType, true);
				} catch (Exception e) {
					errorMsg = "Error initializing '" + storageSystemType.FullName + "': " + e.Message;
					Logger.Error(this, errorMsg);
					throw new Exception(errorMsg);
				}
			}

			if (StoreSystem.StorageType == StorageType.File) {
				// we must be sure to have at least a database path
				DatabasePath = Config.DatabasePath();
				if (String.IsNullOrEmpty(DatabasePath))
					DatabasePath = Config.BasePath();
			}

			// init the storage system
			StoreSystem.Init(this);
		}

		private void SetupCache() {
			// Set up the DataCellCache from the values in the configuration
			string cacheTypeString = Config.GetString(ConfigKeys.CacheType, ConfigDefaultValues.HeapCache);

			if (!String.IsNullOrEmpty(cacheTypeString)) {
				Type cacheType = String.Equals(cacheTypeString, ConfigDefaultValues.HeapCache, StringComparison.InvariantCultureIgnoreCase)
								? typeof(MemoryCache)
								: Type.GetType(cacheTypeString, false, true);

				if (cacheType == null)
					cacheType = typeof(MemoryCache);

				int maxCacheSize = Config.GetInt32(ConfigKeys.DataCacheSize, 0);
				int maxCacheEntrySize = Config.GetInt32(ConfigKeys.MaxCacheEntrySize, 0);

				Logger.Message(this, "Internal Data Cache size:          " + maxCacheSize);
				Logger.Message(this, "Internal Data Cache max cell size: " + maxCacheEntrySize);

				// Find a prime hash size depending on the size of the cache.
				int hashSize = DataCellCache.ClosestPrime(maxCacheSize / 55);

				ICache cache = null;

				try {
					cache = (ICache)Activator.CreateInstance(cacheType, true);
				} catch (Exception e) {
					Logger.Error(this, "An error occurred while initiating cache.");
					Logger.Error(e);
				}

				if (cache == null) {
					Logger.Message(this, "Internal Data Cache disabled.");
				} else {
					cache.Init(Config);

					// Set up the data_cell_cache
					DataCellCache = new DataCellCache(this, cache, maxCacheSize, maxCacheEntrySize, hashSize);
				}
			} else {
				Logger.Message(this, "Internal Data Cache disabled.");
			}
		}

		/// <summary>
		/// Inits the <see cref="SystemContext"/> with the configuration 
		/// properties of the system.
		/// </summary>
		/// <param name="config"></param>
		/// <remarks>
		/// This can only be called once, and should be called at database boot time.
		/// </remarks>
		public virtual void Init(IDbConfig config) {
			functionFactoryList = new ArrayList();
			RoutineResolver = new SystemRoutineResolver();

			if (config != null) {
				this.Config = config;

				// Set the read_only property
				ReadOnlyAccess = config.ReadOnly();

				// Setup the log
				SetupLog();

				SetupStoreSystem();

				// Register the default function factory,
				AddFunctionFactory(SystemFunctions.Factory);
				SetupExternalFunctions();

				// Flush the contents of the function lookup object.
				FlushCachedFunctionLookup();

				SetupCache();

				// Should we open the database in Read only mode?
				Logger.Message(this, "read_only = " + ReadOnlyAccess);
				if (ReadOnlyAccess) Stats.Set("SystemContext.read_only", 1);

				// Generate transaction error if dirty selects are detected?
				TransactionErrorOnDirtySelect = config.TransactionErrorOnDirtySelect();
				Logger.Message(this, "transaction_error_on_dirty_select = " + TransactionErrorOnDirtySelect);

				// Case insensitive identifiers?
				IgnoreIdentifierCase = config.IgnoreIdentifierCase();
				Logger.Message(this, "ignore_case_for_identifiers = " + IgnoreIdentifierCase);

				// What regular expression library are we using?
				// If we want the engine to support other regular expression libraries
				// then include the additional entries here.

				string regexBridge;
				string libUsed;
				string forceLib = config.GetValue<string>(ConfigKeys.ForceRegexLibrary, null);

				// Are we forcing a particular regular expression library?
				if (forceLib != null) {
					libUsed = forceLib;
					// Convert the library string to a class name
					regexBridge = RegexStringToClass(forceLib);
				} else {
					string lib = config.GetString(ConfigKeys.RegexLibrary, ConfigDefaultValues.SystemRegexLibrary);
					libUsed = lib;
					// Convert the library string to a class name
					regexBridge = lib != null ? RegexStringToClass(lib) : ConfigDefaultValues.SystemRegexLibrary;
				}

				if (regexBridge != null) {
					try {
						Type type = Type.GetType(regexBridge);
						regexLibrary = (IRegexLibrary) Activator.CreateInstance(type);
						Logger.Message(this, "Using regex bridge: " + libUsed);
					} catch (Exception e) {
						Logger.Error(this, "Unable to load regex bridge: " + regexBridge);
						Logger.Warning(this, e);
					}
				} else {
					if (libUsed != null)
						Logger.Error(this, "Regex library not known: " + libUsed);
					Logger.Message(this, "Regex features disabled.");
				}
			}
		}

		private void SetupExternalFunctions() {
			try {
				// The 'function_factories' property.
				string functionFactories = Config.GetValue<string>("function_factories", null);
				if (functionFactories != null) {
					string[] factories = functionFactories.Split(';');
					for (int i = 0; i < factories.Length; ++i) {
						string factoryTypeName = factories[i];
						Type type = Type.GetType(factoryTypeName, true, true);
						FunctionFactory functionFactory = (FunctionFactory)Activator.CreateInstance(type);
						AddFunctionFactory(functionFactory);
						Logger.Message(this, "Successfully added function factory: " + factoryTypeName);
					}
				} else {
					Logger.Message(this, "No 'function_factories' config property found.");
					// If resource missing, do nothing...
				}
			} catch (Exception e) {
				Logger.Error(this, "Error parsing 'function_factories' configuration property.");
				Logger.Error(this, e);
			}
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
			lock (functionFactoryList) {
				functionFactoryList.Add(factory);
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
			lock (functionFactoryList) {
				factories = (FunctionFactory[]) functionFactoryList.ToArray(typeof (FunctionFactory));
			}
			((SystemRoutineResolver)RoutineResolver).FlushContents(factories);
		}


		/// <summary>
		///  Creates an event object that is passed into <see cref="PostEvent"/> 
		/// method to run the given <see cref="EventHandler"/> method after the 
		/// time has passed.
		/// </summary>
		/// <param name="callback"></param>
		/// <remarks>
		/// The event created here can be safely posted on the event queue as many
		/// times as you like.  It's useful to create an event as a persistant object
		/// to service some event.  Just post it on the dispatcher when you want
		/// it run!
		/// </remarks>
		/// <returns></returns>
		public object CreateEvent(EventHandler callback) {
			return Dispatcher.CreateEvent(callback);
		}

		/// <summary>
		/// Adds a new event to be dispatched on the queue after 'time_to_wait'
		/// milliseconds has passed.
		/// </summary>
		/// <param name="timeToWait"></param>
		/// <param name="e">An event object returned by <see cref="CreateEvent"/>.</param>
		public void PostEvent(int timeToWait, object e) {
			Dispatcher.PostEvent(timeToWait, e);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (StoreSystem != null)
					StoreSystem.Dispose();
				StoreSystem = null;
				regexLibrary = null;
				DataCellCache = null;
				Config = null;
				LogDirectory = null;
				functionFactoryList = null;
				if (dispatcher != null) {
					dispatcher.Finish();
				}
				//    trigger_manager = null;
				dispatcher = null;
				Logger.Dispose();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}


		// ---------- Inner classes ----------

		#region SystemRoutineResolver

		private sealed class SystemRoutineResolver : IRoutineResolver {
			private FunctionFactory[] factories;

			#region IFunctionLookup Members

			public IRoutine ResolveRoutine(RoutineInvoke routineInvoke, IQueryContext context) {
				lock (this) {
					foreach (FunctionFactory factory in factories) {
						var f = factory.ResolveRoutine(routineInvoke, context);
						if (f != null)
							return f;
					}
					return null;
				}
			}

			public bool IsAggregateFunction(RoutineInvoke routineInvoke, IQueryContext context) {
				lock (this) {
					foreach (FunctionFactory factory in factories) {
						var info = factory.ResolveRoutine(routineInvoke, context) as IFunction;
						if (info != null)
							return info.FunctionType == FunctionType.Aggregate;
					}

					return false;
				}
			}

			#endregion

			public void FlushContents(FunctionFactory[] factories) {
				lock (this) {
					this.factories = factories;
				}
			}
		}

		#endregion
	}
}