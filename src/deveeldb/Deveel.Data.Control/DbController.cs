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
using System.Collections;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data.Control {
	/// <summary>
	/// An object that provides methods for creating and controlling databases
	/// in a given environment.
	/// </summary>
	/// <remarks>
	/// This object keeps an handle on all the databases handled by the system:
	/// when a <see cref="DbSystem"/> returned by either <see cref="CreateDatabase(IDbConfig,string)"/>,
	/// <see cref="StartDatabase(IDbConfig,string)"/> or <see cref="ConnectToDatabase"/> is disposed,
	/// the database associated is still accessible by other threads until the
	/// the <see cref="DbController"/> instance is disposed.
	/// <para>
	/// When the instance of <see cref="DbController"/> is disposed, a call to
	/// <see cref="Database.Shutdown"/> is issued to each database object
	/// referenced.
	/// </para>
	/// </remarks>
	public sealed class DbController : IDatabaseHandler, IDisposable {
		/// <summary>
		/// This object can not be constructed publicaly.
		/// </summary>
		private DbController(IDbConfig configContext) {
			this.configContext = configContext;
			databases = new Hashtable();
			SetupLog(configContext);
		}

		~DbController() {
			Dispose(false);
		}

		private readonly Hashtable databases;
		private readonly IDbConfig configContext;

		/// <summary>
		/// The default name of a database configuration file.
		/// </summary>
		public const string DefaultConfigFileName = "db.conf";

		/// <summary>
		/// The allowed extension for a database configuration file.
		/// </summary>
		public const string FileExtension = ".conf";

		/// <summary>
		/// The event that is fired when a database is <see cref="Database.Shutdown">
		/// shutted down</see>.
		/// </summary>
		public event EventHandler DatabaseShutdown;

		/// <summary>
		/// Gets the main configuration for the controller.
		/// </summary>
		/// <remarks>
		/// The object returned by this property is a copy of the
		/// original: every change to the values will be ignored.
		/// </remarks>
		public IDbConfig Config {
			get { return configContext; }
		}

		/// <summary>
		/// Gets all the names of the databases registered within the
		/// current context.
		/// </summary>
		public string [] Databases {
			get {
				string[] names = new string[databases.Count];
				databases.Keys.CopyTo(names, 0);
				return names;
			}
		}

		public ILogger Logger { get; private set; }

		private void SetupLog(IDbConfig config) {
			//TODO:
			Logger = new EmptyLogger();
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				// if we are still managing databases, call a Shutdown on each one...
				if (databases.Count > 0) {
					foreach (Database database in databases.Values) {
						if (database.IsInitialized)
							database.Shutdown();
						database.Dispose();
					}

					databases.Clear();
				}
			}
		}

		/// <summary>
		/// A dictionary used to cache the pairs of the storage
		/// systems names with their <see cref="StorageType"/>.
		/// </summary>
		private static readonly Dictionary<string, StorageType> StorageTypeCache = new Dictionary<string, StorageType>(30);

		/// <summary>
		/// Gets the type of storage for the system defined in the
		/// configuration given.
		/// </summary>
		/// <param name="config">The <see cref="DbConfig">database 
		/// configuration object</see> that defines the storage system 
		/// for which to retrieve its kind of storage.</param>
		/// <returns>
		/// Returns a <see cref="StorageType"/> defined by the storage
		/// system configured.
		/// </returns>
		/// <seealso cref="GetStorageType(string)"/>
		private static StorageType GetStorageType(IDbConfig config) {
			// if we don't have any configuration given let's assume it's in-memory
			if (config == null)
				return StorageType.Memory;

			string typeName = config.StorageSystem();
			if (typeName == null)
				throw new InvalidOperationException("A storage system must be specified.");

			return GetStorageType(typeName);
		}

		private static StorageType GetStorageType(string typeName) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException("typeName");

			StorageType storageType;
			if (!StorageTypeCache.TryGetValue(typeName, out storageType)) {
				// in case we're using the internal storage system aliases
				if (String.Compare(typeName, ConfigDefaultValues.FileStorageSystem, StringComparison.OrdinalIgnoreCase) == 0)
					storageType = StorageType.File;
				else if (String.Compare(typeName, ConfigDefaultValues.HeapStorageSystem, StringComparison.OrdinalIgnoreCase) == 0)
					storageType = StorageType.Memory;
				else {
					Type type = Type.GetType(typeName, false, true);
					if (type == null)
						throw new InvalidOperationException("The storage system type '" + typeName + "' was not found.");

					if (!typeof(IStoreSystem).IsAssignableFrom(type))
						throw new InvalidOperationException("The type '" + type + "' is not assignable from '" + typeof(IStoreSystem) +
															"'.");

					IStoreSystem storeSystem;

					try {
						storeSystem = (IStoreSystem)Activator.CreateInstance(type, true);
					} catch (Exception e) {
						throw new InvalidOperationException("An error occurred while initializing the type '" + type + "': " + e.Message);
					}

					storageType = storeSystem.StorageType;
				}

				StorageTypeCache[typeName] = storageType;
			}

			return storageType;
		}

		/// <summary>
		/// Creates a new instance of <see cref="DbController"/> to the
		/// given path on the underlying filesystem.
		/// </summary>
		/// <remarks>
		/// If the given path doesn't point to any valid database context
		/// this will generate it by creating a configuration file which
		/// will encapsulate all the default configurations and those
		/// provided in this method.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="DbController"/> which points
		/// to the given path.
		/// </returns>
		public static DbController Create() {
			return Create(DbConfig.Default);
		}

		/// <summary>
		/// Creates a new instance of <see cref="DbController"/> to the
		/// given path on the underlying filesystem.
		/// </summary>
		/// <param name="config">The initial configurations that will
		/// be applied to the subsequent databases within the context.</param>
		/// <remarks>
		/// If the given path doesn't point to any valid database context
		/// this will generate it by creating a configuration file which
		/// will encapsulate all the default configurations and those
		/// provided in this method.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="DbController"/> which points
		/// to the given path.
		/// </returns>
		public static DbController Create(IDbConfig config) {
			StorageType storageType = GetStorageType(config);

			if (config == null)
				config = DbConfig.Default;

			IDbConfig mainConfig;

			if (storageType == StorageType.File) {
				string path = config.BasePath() ?? Environment.CurrentDirectory;

				string configFile = Path.GetFileName(path);

				// we only allow the file extension .conf
				string ext = Path.GetExtension(configFile);
				if (String.Compare(ext, FileExtension, StringComparison.OrdinalIgnoreCase) == 0) {
					path = Path.GetDirectoryName(path);
				} else {
					configFile = DefaultConfigFileName;
				}

				// if the directory doesn't exist we will create one...
				if (path != null && !Directory.Exists(path))
					Directory.CreateDirectory(path);

				mainConfig = GetConfig(config, path, configFile);
				mainConfig.BasePath(path);
			} else {
				mainConfig = config;
			}

			var controller = new DbController(mainConfig);

			if (storageType == StorageType.File) {
				// done with the main configuration... now look for the databases...
				string path = config.BasePath();
				string[] subDirs = Directory.GetDirectories(path);
				foreach (string dir in subDirs) {
					IDbConfig dbConfig = GetConfig(mainConfig, dir, null);
					if (dbConfig == null)
						continue;

					var dbPath = dir.Substring(dir.LastIndexOf(Path.DirectorySeparatorChar) + 1);
					string name = dbConfig.DatabaseName();
					if (name == null)
						name = dbPath;

					dbConfig.DatabasePath(dbPath);

					if (controller.IsDatabaseRegistered(name))
						throw new InvalidOperationException("The database '" + name + "' was already registered.");

					IDatabase database = CreateDatabase(dbConfig, name);
					controller.databases[name] = database;

					if (database.Exists) {
						var callback = new DatabaseShutdownCallback(controller, database);
						database.Context.OnShutdown += (callback.Execute);
					}
				}
			}

			return controller;
		}

		private static IDbConfig GetConfig(IDbConfig parentConfig, string path, string configFile) {
			if (configFile == null)
				configFile = DefaultConfigFileName;

			configFile = Path.Combine(path, configFile);

			bool fileExists = File.Exists(configFile);
			if (!fileExists) {
				// if we didn't find the file where it was supposed to be, try to
				// look for a .conf file into the directory...
				string[] files = Directory.GetFiles(path, "*.conf");

				// if, and only if, we have exactly one .conf file set it as our
				// config file, otherwise we quit...
				configFile = files.Length == 1 ? Path.GetFileName(files[0]) : configFile;
				fileExists = files.Length == 1;
			}

			if (!fileExists)
				return parentConfig;

			// if the config file exists, we load the settings from there...
			//TODO: support more formats

			var config = new DbConfig(parentConfig);
			config.Load(configFile);
			return config;
		}

		/// <summary>
		///  Checks if a database exists within the current context.
		/// </summary>
		/// <param name="name">The name of the database.</param>
		///  <remarks>
		///  Databases are stored in a file-system hieratical way: the
		///  existance of a database within the current context is verified
		///  by combining the name of the database with the path of the
		///  current context.
		///  </remarks>
		/// <returns>
		///  Returns true if a database exists at the given path, false otherwise.
		///  </returns>
		public bool DatabaseExists(string name) {
			return DatabaseExists(null, name);
		}

		/// <summary>
		///  Checks if a database exists within the current context.
		/// </summary>
		/// <param name="config"></param>
		/// <param name="name">The name of the database.</param>
		///  <remarks>
		///  Databases are stored in a file-system hieratical way: the
		///  existance of a database within the current context is verified
		///  by combining the name of the database with the path of the
		///  current context.
		///  </remarks>
		/// <returns>
		///  Returns true if a database exists at the given path, false otherwise.
		///  </returns>
		public bool DatabaseExists(IDbConfig config, string name) {
			// a fast fail...
			var database = databases[name] as Database;
			if (database != null) {
				if (database.IsInitialized)
					return true;

				if (database.Exists)
					return true;
			}

			IDbConfig testConfig;

			if (config == null) {
				testConfig = DbConfig.Default;
			} else {
				testConfig = (IDbConfig) config.Clone();
			}

			testConfig.Parent = Config;

			StorageType storageType = GetStorageType(testConfig);

			if (storageType == StorageType.File) {
				var basePath = testConfig.BasePath();
				if (String.IsNullOrEmpty(basePath))
					basePath = ConfigDefaultValues.BasePath;

				// we ensure that the BasePath points to where we want it to point
				string path = Path.Combine(basePath, name);
				return Directory.Exists(path);
			}

			return false;
		}

		private bool IsDatabaseRegistered(string name) {
			return databases.ContainsKey(name);
		}

		/// <inheritdoc/>
		public IDatabase GetDatabase(string name) {
			return databases[name] as Database;
		}

		/// <summary>
		/// Verifies if the database identified by the given name
		/// was already initialized (by either <see cref="StartDatabase(DbConfig, string)"/>
		/// or <see cref="CreateDatabase(DbConfig, string)"/>).
		/// </summary>
		/// <param name="databaseName">The name of the database to test.</param>
		/// <returns>
		/// Returns <b>true</b> if the database with the given name was already
		/// initialized, or <b>false</b> if the database was not found or it was
		/// not initialized.
		/// </returns>
		public bool IsInitialized(string databaseName) {
			IDatabase database = GetDatabase(databaseName);
			return (database != null && database.IsInitialized);
		}

		/// <summary>
		/// Creates a new database with the given name and with initial
		/// configurations.
		/// </summary>
		/// <param name="name">The name of the database to create.</param>
		/// <param name="adminUser">The name of the administrator for the database,</param>
		/// <param name="adminPass">The password used to identify the administrator.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database created.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> of the database is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> already exists.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while initializing the database.
		/// </exception>
		public DbSystem CreateDatabase(string name, string adminUser, string adminPass) {
			return CreateDatabase(null, name, adminUser, adminPass);
		}

		/// <summary>
		/// Creates a new database with the given name and with initial
		/// configurations.
		/// </summary>
		/// <param name="config">The configurations specific for the dataabse
		/// to create. These will be merged with existing context configurations.</param>
		/// <param name="name">The name of the database to create.</param>
		/// <param name="adminUser">The name of the administrator for the database,</param>
		/// <param name="adminPass">The password used to identify the administrator.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database created.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> of the database is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> already exists.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while initializing the database.
		/// </exception>
		public DbSystem CreateDatabase(IDbConfig config, string name, string adminUser, string adminPass) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (config == null)
				config = DbConfig.Default;

			if (DatabaseExists(config, name))
				throw new ArgumentException("A database '" + name + "' already exists.");

			config.Parent = Config;

			StorageType storageType = GetStorageType(config);

			if (storageType == StorageType.File) {
				// we ensure that the BasePath points to where we want it to point
				string path = Path.Combine(config.BasePath(), name);
				if (Directory.Exists(path))
					throw new ApplicationException("Database path '" + name + "' already exists: try opening");

				Directory.CreateDirectory(path);

				config.SetValue(ConfigKeys.DatabasePath, name);

				string configFile = Path.Combine(path, DefaultConfigFileName);
				//TODO: support multiple formats?
				config.Save(configFile);
			}

			IDatabase database = CreateDatabase(config, name);

			try {
				database.Create(adminUser, adminPass);
				database.Init();

				var callback = new DatabaseShutdownCallback(this, database);
				database.Context.OnShutdown += (callback.Execute);
			} catch (Exception e) {
				database.Context.Logger.Error(this, "Database create failed");
				database.Context.Logger.Error(this, e);
				throw new InvalidOperationException(e.Message, e);
			}
			// Return the DbSystem object for the newly created database.
			databases[name] = database;
			return new DbSystem(this, name, config, database);
		}

		public bool DeleteDatabase(string name, string adminName, string adminPass) {
			return DeleteDatabase(null, name, adminName, adminPass);
		}

		public bool DeleteDatabase(IDbConfig config, string name, string adminName, string adminPass) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (config == null)
				config = DbConfig.Default;

			if (!DatabaseExists(config, name))
				return false;

			config.Parent = Config;

			IDatabase database = GetDatabase(name);
			if (database == null)
				return false;
			
			// TODO: query the db to see if the user is the admin

			try {
				//TODO: close all connections to the database

				try {
					if (database.IsInitialized)
					database.Shutdown();
				} finally {
					database.Dispose();

					if (databases.ContainsKey(name))
						databases.Remove(name);
				}			
			} catch (Exception ex) {
				Logger.Error(ex);
			}

			StorageType storageType = GetStorageType(config);

			if (storageType == StorageType.File) {
				// we ensure that the BasePath points to where we want it to point
				string path = Path.Combine(config.BasePath(), name);
				if (!Directory.Exists(path))
					return false;

				Directory.Delete(path, true);
			}

			return true;
		}

		/// <summary>
		/// Starts up the database identified with the given name.
		/// </summary>
		/// <param name="name">The name of the database to start.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database created.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> of the database is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> already exists.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while initializing the database.
		/// </exception>
		public DbSystem StartDatabase(string name) {
			return StartDatabase(null, name);
		}

		/// <summary>
		/// Starts up the database identified with the given name.
		/// </summary>
		/// <param name="config">The configurations used to start the database.</param>
		/// <param name="name">The name of the database to start.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database created.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> of the database is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> already exists.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while initializing the database.
		/// </exception>
		public DbSystem StartDatabase(IDbConfig config, string name) {
			if (!DatabaseExists(config, name))
				throw new ArgumentException("Database '" + name + "' not existing.", "name");

			if (config == null)
				config = DbConfig.Default;

			config.Parent = Config;

			IDatabase database = GetDatabase(name);

			if (database.IsInitialized)
				throw new ArgumentException("The database is already initialized.");

			// First initialise the database
			try {
				database.Init();
			} catch (DatabaseException e) {
				database.Context.Logger.Error(this, "Database init failed");
				database.Context.Logger.Error(this, e);
				throw new InvalidOperationException(e.Message);
			}

			// Return the DbSystem object for the newly created database.
			return new DbSystem(this, name, config, database);
		}

		/// <summary>
		/// Connects to the database identified by the name given.
		/// </summary>
		/// <param name="config"></param>
		/// <param name="name">The name of the database to connect to.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> does not exist.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> provided is <b>null</b>.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the database was not initialized.
		/// </exception>
		public DbSystem ConnectToDatabase(string name) {
			return ConnectToDatabase(null, name);
		}

		/// <summary>
		/// Connects to the database identified by the name given.
		/// </summary>
		/// <param name="config"></param>
		/// <param name="name">The name of the database to connect to.</param>
		/// <returns>
		/// Returns a <see cref="DbSystem"/> instance used to access the database.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// If a database with the given <paramref name="name"/> does not exist.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> provided is <b>null</b>.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the database was not initialized.
		/// </exception>
		public DbSystem ConnectToDatabase(IDbConfig config, string name) {
			if (config == null)
				config = DbConfig.Default;

			if (!DatabaseExists(config, name))
				throw new ArgumentException("Database '" + name + "' not existing.", "name");

			IDatabase database = GetDatabase(name);
			if (!database.IsInitialized)
				throw new InvalidOperationException("The database is not initialized.");

			return new DbSystem(this, name, database.Context.Config, database);
		}

		// ---------- Static methods ----------

		/// <summary>
		/// Creates a Database object for the given DbConfig configuration.
		/// </summary>
		/// <param name="config"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		private static IDatabase CreateDatabase(IDbConfig config, string name) {
			var context = new DatabaseContext();

			// Initialize the DatabaseSystem first,
			// ------------------------------------

			// This will throw an Error exception if the database system has already
			// been initialized.
			context.Init(config);

			// Start the database class
			// ------------------------

			var database = new Database(context, name);

			// Start up message
			database.Context.Logger.Trace(typeof(DbController), "Starting Database Server");

			return database;
		}

		private void OnDatabaseShutdown(IDatabase database) {
			databases.Remove(database.Name);

			if (DatabaseShutdown != null)
				DatabaseShutdown(this, EventArgs.Empty);
		}

		private class DatabaseShutdownCallback {
			public DatabaseShutdownCallback(DbController controller, IDatabase database) {
				this.controller = controller;
				this.database = database;
			}

			private readonly IDatabase database;
			private readonly DbController controller;

			public void Execute(object sender, EventArgs args) {
				controller.OnDatabaseShutdown(database);
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}