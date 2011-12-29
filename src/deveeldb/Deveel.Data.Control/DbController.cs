// 
//  Copyright 2010-2011 Deveel
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

using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data.Control {
	/// <summary>
	/// An object that provides methods for creating and controlling databases
	/// in a given environment.
	/// </summary>
	/// <remarks>
	/// This object keeps an handle on all the databases handled by the system:
	/// when a <see cref="DbSystem"/> returned by either <see cref="CreateDatabase"/>,
	/// <see cref="StartDatabase"/> or <see cref="ConnectToDatabase"/> is disposed,
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
		private DbController(DbConfig configContext) {
			this.configContext = configContext;
			databases = new Hashtable();
			SetupLog(configContext);
		}

		~DbController() {
			Dispose(false);
		}

		private readonly Hashtable databases;
		private readonly DbConfig configContext;
		private Logger logger;

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
		public DbConfig Config {
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

		public Logger Logger {
			get { return logger; }
		}

		private void SetupLog(DbConfig config) {
			//TODO:
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				// if we are still managing databases, call a Shutdown on each one...
				if (databases.Count > 0) {
					foreach (Database database in databases.Values) {
						database.Shutdown();
					}

					databases.Clear();
				}
			}
		}

		/// <summary>
		/// A dictionary used to cache the pairs of the storage
		/// systems names with their <see cref="StorageType"/>.
		/// </summary>
		private static readonly Dictionary<string, StorageType> cache = new Dictionary<string, StorageType>(30);

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
		private static StorageType GetStorageType(DbConfig config) {
			// if we don't have any configuration given let's assume it's in-memory
			if (config == null)
				return StorageType.Memory;

			string typeName = config.GetValue(ConfigKeys.StorageSystem);
			if (typeName == null)
				throw new InvalidOperationException("A storage system must be specified.");

			return GetStorageType(typeName);
		}

		private static StorageType GetStorageType(string typeName) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException("typeName");

			StorageType storageType;
			if (!cache.TryGetValue(typeName, out storageType)) {
				// in case we're using the internal storage system aliases
				if (String.Compare(typeName, "v1file", true) == 0)
					storageType = StorageType.File;
				else if (String.Compare(typeName, "v1heap", true) == 0)
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

				cache[typeName] = storageType;
			}

			return storageType;
		}


		public static DbController Create(DbConfig config) {
			return Create(null, config);
		}

		/// <summary>
		/// Creates a new instance of <see cref="DbController"/> to the
		/// given path on the underlying filesystem.
		/// </summary>
		/// <param name="path">The root path where to instantiate the
		/// <see cref="DbController"/>.</param>
		/// <returns>
		/// Returns an instance of <see cref="DbController"/> which points
		/// to the given path.
		/// </returns>
		/// <seealso cref="Create(string,Deveel.Data.Control.DbConfig)"/>
		public static DbController Create(string path) {
			return Create(path, null);
		}

		/// <summary>
		/// Creates a new instance of <see cref="DbController"/> to the
		/// given path on the underlying filesystem.
		/// </summary>
		/// <param name="path">The root path where to instantiate the
		/// <see cref="DbController"/>.</param>
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
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="path"/> provided is <b>null</b>.
		/// </exception>
		public static DbController Create(string path, DbConfig config) {
			StorageType storageType = GetStorageType(config);

			if (config == null)
				config = new DbConfig();

			DbConfig mainConfig;

			if (storageType == StorageType.File) {
				if (path == null)
					path = Environment.CurrentDirectory;
				
				string configFile = Path.GetFileName(path);
				if (configFile == null) {
					configFile = DefaultConfigFileName;
				} else {
					// we only allow the file extension .conf
					string ext = Path.GetExtension(configFile);
					if (String.Compare(ext, FileExtension, true) == 0) {
						path = Path.GetDirectoryName(path);
					} else {
						configFile = DefaultConfigFileName;
					}
				}

				// if the directory doesn't exist we will create one...
				if (path != null && !Directory.Exists(path))
					Directory.CreateDirectory(path);

				mainConfig = GetConfig((DbConfig)config.Clone(), path, configFile);
			} else {
				mainConfig = (DbConfig) config.Clone();
			}

			DbController controller = new DbController(mainConfig);

			if (storageType == StorageType.File) {
				// done with the main configuration... now look for the databases...
				string[] subDirs = Directory.GetDirectories(path);
				foreach (string dir in subDirs) {
					DbConfig dbConfig = GetConfig(mainConfig, dir, null);
					if (dbConfig == null)
						continue;

					string name = dbConfig.GetValue("name");
					if (name == null)
						name = dir.Substring(dir.LastIndexOf(Path.DirectorySeparatorChar) + 1);

					if (controller.DatabaseExists(name))
						throw new InvalidOperationException("The database '" + name + "' was already registered.");

					Database database = CreateDatabase(dbConfig, name);
					if (database.Exists) {
						DatabaseShutdownCallback callback = new DatabaseShutdownCallback(controller, database);
						database.RegisterShutDownDelegate(new EventHandler(callback.Execute));
						controller.databases[name] = database;
					}
				}
			}

			return controller;
		}

		private static DbConfig GetConfig(DbConfig parentConfig, string path, string configFile) {
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
				return null;

			DbConfig config = new DbConfig();
			config.CurrentPath = path;

			if (parentConfig != null)
				config.Merge(parentConfig);

			// if the config file exists, we load the settings from there...
			config.LoadFromFile(configFile);

			return config;
		}

		///<summary>
		/// Checks if a database exists within the current context.
		///</summary>
		/// <param name="name">The name of the database.</param>
		/// <remarks>
		/// Databases are stored in a file-system hieratical way: the
		/// existance of a database within the current context is verified
		/// by combining the name of the database with the path of the
		/// current context.
		/// </remarks>
		///<returns>
		/// Returns true if a database exists at the given path, false otherwise.
		/// </returns>
		public bool DatabaseExists(string name) {
			// a fast fail...
			Database database = databases[name] as Database;
			if (database == null)
				return false;
			if (database.IsInitialized)
				return true;

			return database.Exists;
		}

		/// <inheritdoc/>
		public Database GetDatabase(string name) {
			return databases[name] as Database;
		}

		/// <summary>
		/// Verifies if the database identified by the given name
		/// was already initialized (by either <see cref="StartDatabase"/>
		/// or <see cref="CreateDatabase"/>).
		/// </summary>
		/// <param name="databaseName">The name of the database to test.</param>
		/// <returns>
		/// Returns <b>true</b> if the database with the given name was already
		/// initialized, or <b>false</b> if the database was not found or it was
		/// not initialized.
		/// </returns>
		public bool IsInitialized(string databaseName) {
			Database database = GetDatabase(databaseName);
			return (database != null && database.IsInitialized);
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
		public DbSystem CreateDatabase(DbConfig config, string name, string adminUser, string adminPass) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (DatabaseExists(name))
				throw new ArgumentException("A database '" + name + "' already exists.");

			StorageType storageType = GetStorageType(config);

			string path = "";
			DbConfig dbConfig = new DbConfig();
			if (storageType == StorageType.File) {
				// we ensure that the CurrentPath points to where we want it to point
				path = Path.Combine(Config.CurrentPath, name);
				if (Directory.Exists(path))
					Directory.Delete(path);

				Directory.CreateDirectory(path);

				dbConfig.CurrentPath = path;
			}

			dbConfig.Merge(Config);
			if (config != null)
				dbConfig.Merge(config);

			if (storageType == StorageType.File) {
				string configFile = Path.Combine(path, DefaultConfigFileName);
				dbConfig.SaveTo(configFile);
			}

			Database database = CreateDatabase(dbConfig, name);

			try {
				database.Create(adminUser, adminPass);
				database.Init();
				DatabaseShutdownCallback callback = new DatabaseShutdownCallback(this, database);
				database.RegisterShutDownDelegate(callback.Execute);
			} catch (Exception e) {
				database.Logger.Error(this, "Database create failed");
				database.Logger.Error(this, e);
				throw new InvalidOperationException(e.Message);
			}
			// Return the DbSystem object for the newly created database.
			databases[name] = database;
			return new DbSystem(this, name, config, database);
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
		public DbSystem StartDatabase(DbConfig config, string name) {
			if (!DatabaseExists(name))
				throw new ArgumentException("Database '" + name + "' not existing.", "name");

			string path = Path.Combine(Config.CurrentPath, name);

			DbConfig dbConfig = new DbConfig();
			dbConfig.CurrentPath = path;
			dbConfig.Merge(Config);
			if (config != null)
				dbConfig.Merge(config);

			Database database = GetDatabase(name);

			if (database.IsInitialized)
				throw new ArgumentException("The database is already initialized.");

			// First initialise the database
			try {
				database.Init();
			} catch (DatabaseException e) {
				database.Logger.Error(this, "Database init failed");
				database.Logger.Error(this, e);
				throw new InvalidOperationException(e.Message);
			}

			// Return the DbSystem object for the newly created database.
			return new DbSystem(this, name, dbConfig, database);
		}

		/// <summary>
		/// Connects to the database identified by the name given.
		/// </summary>
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
			if (!DatabaseExists(name))
				throw new ArgumentException("Database '" + name + "' not existing.", "name");

			Database database = GetDatabase(name);
			if (!database.IsInitialized)
				throw new InvalidOperationException("The database is not initialized.");

			return new DbSystem(this, name, database.System.Config, database);
		}

		// ---------- Static methods ----------

		/// <summary>
		/// Creates a Database object for the given IDbConfig configuration.
		/// </summary>
		/// <param name="config"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		private static Database CreateDatabase(DbConfig config, string name) {
			DatabaseSystem system = new DatabaseSystem();

			// Initialize the DatabaseSystem first,
			// ------------------------------------

			// This will throw an Error exception if the database system has already
			// been initialized.
			system.Init(config);

			// Start the database class
			// ------------------------

			Database database = new Database(system, name);

			// Start up message
			database.Logger.Message(typeof(DbController), "Starting Database Server");

			return database;
		}

		private void OnDatabaseShutdown(Database database) {
			databases.Remove(database.Name);

			if (DatabaseShutdown != null)
				DatabaseShutdown(this, EventArgs.Empty);
		}

		private class DatabaseShutdownCallback {
			public DatabaseShutdownCallback(DbController controller, Database database) {
				this.controller = controller;
				this.database = database;
			}

			private readonly Database database;
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