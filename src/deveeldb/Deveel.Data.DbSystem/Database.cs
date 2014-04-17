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
using Deveel.Data.Control;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Store;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// The representation of a single database in the system.
	/// </summary>
	/// <remarks>
	/// A database is a set of schema, tables, definitions of tables in 
	/// the schemas, and descriptions of schemas.
	/// <para>
	/// This class encapsulates the top level behaviour of a database. That is
	/// of creating itself, initializing itself, shutting itself down, deleting
	/// itself, creating/dropping a table, updating a table. It is not the 
	/// responsibility of this class to handle table behaviour above this. Top
	/// level table behaviour is handled by <see cref="DataTable"/> through the 
	/// <see cref="DatabaseConnection"/> interface.
	/// </para>
	/// <para>
	/// The <see cref="Database"/> object is also responsible for various 
	/// database management functions such a creating, editing and removing 
	/// users, triggers, functions and services.
	/// </para>
	/// </remarks>
	public sealed partial class Database : IDatabase {
		// ---------- Statics ----------

		/// <summary>
		/// The name of the default schema.
		/// </summary>
		public const String DefaultSchema = "APP";

		/// <summary>
		/// A flag which, when set to true, will cause the engine to delete the
		/// database from the file system when it is shut down.
		/// </summary>
		private bool deleteOnShutdown;

		///<summary>
		///</summary>
		///<param name="context"></param>
		///<param name="name"></param>
		public Database(DatabaseContext context, string name) {
			this.Context = context;
			deleteOnShutdown = false;
			this.Name = name;
			context.RegisterDatabase(this);
			Conglomerate = new TableDataConglomerate(context, name, context.StoreSystem);
			InternalSystemUser = new User(User.SystemName, this, "", DateTime.Now);

			// Create the single row table
			TemporaryTable t = new TemporaryTable(this,"SINGLE_ROW_TABLE", new DataColumnInfo[0]);
			t.NewRow();
			SingleRowTable = t;
		}

		~Database() {
			Dispose(false);
		}

		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Returns true if this database is in read-only mode.
		/// </summary>
		public bool IsReadOnly {
			get { return Context.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the internal system user for this database.
		/// </summary>
		private User InternalSystemUser { get; set; }

		// ---------- Log accesses ----------

		/// <summary>
		/// Returns the log file where commands are recorded.
		/// </summary>
		public Log CommandsLog { get; private set; }

		/// <summary>
		/// Returns the conglomerate for this database.
		/// </summary>
		internal TableDataConglomerate Conglomerate { get; private set; }

		/// <summary>
		/// Gets <b>true</b> if the database exists.
		/// </summary>
		/// <remarks>
		/// The test should be called before <see cref="Init"/> method to check
		/// if the database already exists.
		/// </remarks>
		public bool Exists {
			get {
				if (IsInitialized)
					throw new Exception("The database is initialised, so no point testing it's existance.");

				try {
					// HACK: If the legacy style '.sf' state file exists then we must return
					//   true here because technically the database exists but is not in the
					//   correct version.
					if (Conglomerate.Exists())
						return true;

					if (Context.StoreSystem.StorageType == StorageType.File &&
					    File.Exists(Path.Combine(Context.DatabasePath, Name + ".sf")))
						return true;

					return false;
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new Exception("IO Error: " + e.Message, e);
				}
			}
		}

		/// <summary>
		/// Returns true if the database is initialised.
		/// </summary>
		public bool IsInitialized { get; private set; }

		/// <summary>
		/// Returns the <see cref="DatabaseContext"/> that this Database is from.
		/// </summary>
		public DatabaseContext Context { get; private set; }

		IDatabaseContext IDatabase.Context {
			get { return Context; }
		}

		/// <summary>
		/// Returns the IStoreSystem for this Database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return Context.StoreSystem; }
		}

		/// <summary>
		/// Convenience static for accessing the global Stats object.
		/// </summary>
		// Perhaps this should be deprecated?
		public Stats Stats {
			get { return Context.Stats; }
		}

		/// <summary>
		/// Returns the system user manager.
		/// </summary>
		public UserManager UserManager {
			get { return Context.UserManager; }
		}

		/// <summary>
		/// Returns the system DataCellCache.
		/// </summary>
		internal DataCellCache DataCellCache {
			get { return Context.DataCellCache; }
		}

		/// <summary>
		/// Returns true if the database has shut down.
		/// </summary>
		public bool HasShutDown {
			get { return Context.HasShutDown; }
		}

		/// <summary>
		/// Returns a static table that has a single row but no columns.
		/// </summary>
		/// <remarks>
		/// This table is useful for certain database operations.
		/// </remarks>
		public Table SingleRowTable { get; private set; }

		/// <summary>
		/// Gets the <see cref="ILogger"/> implementation from the parent 
		/// <see cref="DatabaseContext"/> context.
		/// </summary>
		public Logger Logger {
			get { return Context.Logger; }
		}

		/// <summary>
		/// Returns a new <see cref="DatabaseConnection"/> instance that is 
		/// used against this database.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="triggerCallback"></param>
		/// <remarks>
		/// When a new connection is made on this database, this method is 
		/// called to create a new <see cref="DatabaseConnection"/> instance 
		/// for the connection. This connection handles all transactional 
		/// queries and modifications to the database.
		/// </remarks>
		/// <returns></returns>
		public DatabaseConnection CreateNewConnection(User user, TriggerCallback triggerCallback) {
			if (user == null)
				user = InternalSystemUser;

			var connection = new DatabaseConnection(this, user, triggerCallback);
			// Initialize the connection
			connection.Init();

			return connection;
		}


		// ---------- Schema management ----------

		private static void CreateSchemata(DatabaseConnection connection) {
			connection.CreateSchema(DefaultSchema, "DEFAULT");
			connection.CreateSchema(InformationSchema.Name, "SYSTEM");
		}

		/// <summary>
		/// Set up the system table grants.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="grantee"></param>
		/// <remarks>
		/// This gives the grantee user full access to <i>passwords</i>,
		/// <i>user_privs</i>, <i>user_connect_privs</i>, <i>services</i>, 
		/// <i>function_factories</i>, and functions. All other system 
		/// tables are granted <i>SELECT</i> only.
		/// </remarks>
		private static void SetSystemGrants(DatabaseConnection connection, string grantee) {
			const string granter = User.SystemName;

			// Add all priv grants to those that the system user is allowed to change
			GrantManager manager = connection.GrantManager;

			// Add schema grant for APP
			manager.Grant(Privileges.SchemaAll, GrantObject.Schema, DefaultSchema, grantee, true, granter);
			// Add public grant for SYSTEM
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, SystemSchema.Name, User.PublicName, false, granter);
			// Add public grant for INFORMATION_SCHEMA
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, InformationSchema.Name, User.PublicName, false, granter);

			// For all tables in the SYSTEM schema, grant all privileges to the
			// system user.
			manager.GrantToAllTablesInSchema(SystemSchema.Name, Privileges.TableAll, grantee, false, granter);


			SystemSchema.SetTableGrants(manager, granter);
			InformationSchema.SetViewsGrants(manager, granter);
		}

		private const string DataVersionFileName = "Deveel.Data.DataVersion";

		private Version ReadDataVersionFromFile() {
			//TODO: Read the embedded file for the real version ...
			return new Version(1, 1);
		}

		private void AssertDataVersion() {
			var dataVersion = ReadDataVersionFromFile();

			// Check the state of the conglomerate,
			DatabaseConnection connection = CreateNewConnection(null, null);
			var context = new DatabaseQueryContext(connection);
			connection.LockingMechanism.SetMode(LockingMode.Exclusive);
			if (!connection.TableExists(SystemSchema.PersistentVarTable)) {
				throw new DatabaseException(
					"The database_vars table doesn't exist.  This means the " +
					"database is pre-schema version 1 or the table has been deleted." +
					"If you are converting an old version of the database, please " +
					"convert the database using an older release.");
			}

			// What version is the data?
			DataTable databaseVars = connection.GetTable(SystemSchema.PersistentVarTable);
			IDictionary vars = databaseVars.ToDictionary();
			var dbVersion = new Version(vars["database.version"].ToString());
			// If the version doesn't equal the current version, throw an error.
			if (!dbVersion.Equals(dataVersion)) {
				throw new DatabaseException(
					"Incorrect data file version '" + dbVersion + "'.  Please see " +
					"the README on how to convert the data files to the current " +
					"version.");
			}

			// Commit and close the connection.
			connection.Commit();
			connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
			connection.Close();
		}

		private void SetCurrentDataVersion(DatabaseConnection transaction) {
			var version = ReadDataVersionFromFile();

			// Insert the version number of the database
			transaction.SetPersistentVariable("database.version", version.ToString(2));
		}

		/// <summary>
		/// Creates and sets up a new database to an initial empty state. 
		/// </summary>
		/// <param name="username">The username for the database administrator.</param>
		/// <param name="password">The database administrator password.</param>
		/// <remarks>
		/// The creation process involves creating all the system tables and 
		/// views, adding an administrator user account, creating schema, and 
		/// setting up the initial grant information for the administrator user.
		/// </remarks>
		public void Create(String username, String password) {
			if (IsReadOnly) {
				throw new Exception("Can not create database in Read only mode.");
			}

			if (String.IsNullOrEmpty(username) ||
			    String.IsNullOrEmpty(password))
				throw new Exception("Must have valid username and password String");

			try {
				// Create the conglomerate
				Conglomerate.Create();

				DatabaseConnection connection = CreateNewConnection(null, null);
				DatabaseQueryContext context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				connection.CurrentSchema = SystemSchema.Name;

				// Create the schema information tables introduced in version 0.90
				// and 0.94
				CreateSchemata(connection);

				// The system tables that are present in every conglomerate.
				SystemSchema.CreateTables(connection);

				// Create the system views
				InformationSchema.CreateSystemViews(connection, Logger);

				// Creates the administrator user.
				CreateUser(context, username, password);
				// This is the admin user so add to the 'secure access' table.
				AddUserToGroup(context, username, SystemGroupNames.SecureGroup);
				// Allow all localhost TCP connections.
				// NOTE: Permissive initial security!
				GrantHostAccessToUser(context, username, "TCP", "%");
				// Allow all Local connections.
				GrantHostAccessToUser(context, username, "Local", "%");

				// Sets the system grants for the administrator
				SetSystemGrants(connection, username);

				SetCurrentDataVersion(connection);

				// Set all default system procedures.
				SystemSchema.SetupSystemFunctions(connection, username);

				try {
					// Close and commit this transaction.
					connection.Commit();
				} catch (TransactionException e) {
					Logger.Error(this, e);
					throw new ApplicationException("Transaction Error: " + e.Message, e);
				}

				connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
				connection.Close();

				// Close the conglomerate.
				Conglomerate.Close();
			} catch (DatabaseException e) {
				Logger.Error(this, e);
				throw new ApplicationException("Database Exception: " + e.Message, e);
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new ApplicationException("IO Error: " + e.Message, e);
			}
		}

		/// <summary>
		/// Opens and initializes the database.
		/// </summary>
		/// <remarks>
		/// This opens all the files that are required for the operation 
		/// of the database.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If it finds the version of the data files are not a compatible 
		/// version or if the database is already opened.
		/// </exception>
		/// <exception cref="IOException">
		/// If any IO error occurred during the opening process.
		/// </exception>
		public void Init() {
			if (IsInitialized)
				throw new Exception("Init() method can only be called once.");

			// Reset all session statistics.
			Stats.ResetSession();

			try {
				string logPath = this.Context.LogDirectory;
				if (logPath != null && this.Context.LogQueries) {
					CommandsLog = new Log(Path.Combine(logPath, "commands.log"), 256*1024, 5);
				} else {
					CommandsLog = Log.Null;
				}

				// Check if the state file exists.  If it doesn't, we need to report
				// incorrect version.
				if (!StoreSystem.StoreExists(Name + "_sf")) {
					// If state store doesn't exist but the legacy style '.sf' state file
					// exists,
					if (this.Context.DatabasePath != null &&
					    File.Exists(Path.Combine(this.Context.DatabasePath, Name + ".sf"))) {
						throw new DatabaseException(
							"The state store for this database doesn't exist.  This means " +
							"the database version is pre version 1.0.  Please see the " +
							"README for the details for converting this database.");
					}

					// If neither store or state file exist, assume database doesn't
					// exist.
					throw new DatabaseException("The database does not exist.");
				}

				// Open the conglomerate
				Conglomerate.Open();

				AssertDataVersion();
			} catch (TransactionException e) {
				// This would be very strange error to receive for in initializing
				// database...
				throw new ApplicationException("Transaction Error: " + e.Message);
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new ApplicationException("IO Error: " + e.Message);
			}

			IsInitialized = true;
		}

		/// <summary>
		/// Cleanly shuts down the database.
		/// </summary>
		/// <remarks>
		/// It is important that this method is called just before the system 
		/// closes down.
		/// <para>
		/// The main purpose of this method is to ensure any tables that are 
		/// backed by files and in a <i>safe</i> state and cleanly flushed to 
		/// the file system.
		/// </para>
		/// <para>
		/// If <see cref="deleteOnShutdown"/> is true, the database will delete itself from the file 
		/// system when it shuts down.
		/// </para>
		/// </remarks>
		public void Shutdown() {
			if (IsInitialized == false) {
				throw new ApplicationException("The database is not initialized.");
			}

			try {
				if (deleteOnShutdown) {
					// Delete the conglomerate if the database is set to delete on
					// shutdown.
					Conglomerate.Delete();
				} else {
					// Otherwise close the conglomerate.
					Conglomerate.Close();
				}
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new ApplicationException("IO Error: " + e.Message, e);
			}

			// Shut down the logs...
			if (CommandsLog != null) {
				CommandsLog.Close();
			}

			IsInitialized = false;
		}

		///<summary>
		/// If the 'delete_on_shutdown' flag is set, the database will delete 
		/// the database from the file system when it is shutdown.
		///</summary>
		///<param name="status"></param>
		/// <remarks>
		/// <b>Note</b>: Use with care - if this is set to true and the database is 
		/// shutdown it will result in total loss of data.
		/// </remarks>
		public void SetDeleteOnShutdown(bool status) {
			deleteOnShutdown = status;
		}

		/// <summary>
		/// Copies all the persistent data in this database (the conglomerate) to 
		/// the given destination path.
		/// </summary>
		/// <param name="path">The destination path of the copy.</param>
		/// <remarks>
		///  This method can copy information while the database is <i>live</i>.
		/// </remarks>
		public void LiveCopyTo(string path) {
			if (IsInitialized == false)
				throw new ApplicationException("The database is not initialized.");

			// Set up the destination conglomerate to copy all the data to,
			// Note that this sets up a typical destination conglomerate and changes
			// the cache size and disables the debug log.
			SystemContext copyContext = new SystemContext();
			var config = new DbConfig();
			config.DatabasePath(Path.GetFullPath(path));
			config.LogPath("");
			config.SetValue(ConfigKeys.DebugLevel, 50000);
			// Set data cache to 1MB
			config.SetValue(ConfigKeys.DataCacheSize, "1048576");
			// Set io_safety_level to 1 for destination database
			// ISSUE: Is this a good assumption to make - 
			//     we don't care if changes are lost by a power failure when we are
			//     backing up the database.  Even if journalling is enabled, a power
			//     failure will lose changes in the backup copy anyway.
			config.SetValue("io_safety_level", "1");
			config.SetValue(ConfigKeys.DebugLogs, "disabled");
			copyContext.Init(config);

			TableDataConglomerate destConglomerate = new TableDataConglomerate(copyContext, Name, copyContext.StoreSystem);

			// Open the congloemrate
			destConglomerate.MinimalCreate();

			try {
				// Make a copy of this conglomerate into the destination conglomerate,
				Conglomerate.LiveCopyTo(destConglomerate);
			} finally {
				// Close the congloemrate when finished.
				destConglomerate.Close();
				// Dispose the TransactionSystem
				copyContext.Dispose();
			}
		}

		// ---------- System access ----------

		/// <summary>
		/// Creates an event for the database dispatcher.
		/// </summary>
		/// <param name="runner"></param>
		/// <returns></returns>
		public object CreateEvent(EventHandler runner) {
			return Context.CreateEvent(runner);
		}

		/// <summary>
		/// Posts an event on the database dispatcher.
		/// </summary>
		/// <param name="time"></param>
		/// <param name="e"></param>
		public void PostEvent(int time, Object e) {
			Context.PostEvent(time, e);
		}


		/// <summary>
		/// Starts the shutdown thread which should contain delegates that shut the
		/// database and all its resources down.
		/// </summary>
		/// <remarks>
		/// This method returns immediately.
		/// </remarks>
		public void StartShutDownThread() {
			Context.StartShutDownThread();
		}

		/// <summary>
		/// Blocks until the database has shut down.
		/// </summary>
		public void WaitUntilShutdown() {
			Context.WaitUntilShutdown();
		}

		/// <summary>
		/// Executes database functions from the given 
		/// delegate on the first available worker thread.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database"></param>
		/// <param name="runner"></param>
		/// <remarks>
		/// All database functions must go through a worker thread.  If we 
		/// ensure this, we can easily stop all database functions from executing 
		/// if need be.  Also, we only need to have a certain number of threads 
		/// active at any one time rather than a unique thread for each connection.
		/// </remarks>
		public void Execute(User user, DatabaseConnection database, EventHandler runner) {
			Context.Execute(user, database, runner);
		}

		/// <summary>
		/// Registers the delegate that is executed when the shutdown 
		/// thread is activated.
		/// </summary>
		/// <param name="e"></param>
		public void RegisterShutDownDelegate(EventHandler e) {
			Context.RegisterShutDownDelegate(e);
		}

		/// <summary>
		/// Controls whether the database is allowed to execute commands or not.
		/// </summary>
		/// <remarks>
		/// If this is set to true, then calls to 'execute' will be executed
		/// as soon as there is a free worker thread available.  Otherwise no
		/// commands are executed until this is enabled.
		/// </remarks>
		public void SetIsExecutingCommands(bool status) {
			Context.SetIsExecutingCommands(status);
		}

		#region Implementation of IDisposable

		private void Dispose(bool disposing) {
			if (disposing) {
				if (IsInitialized)
					Console.Error.WriteLine("Database object was finalized and is initialized!");

				GC.SuppressFinalize(this);
			}
		}

		public void Dispose() {
			Dispose(true);
		}

		#endregion
	}
}