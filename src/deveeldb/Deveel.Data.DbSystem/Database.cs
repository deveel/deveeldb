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
using Deveel.Data.Control;
using Deveel.Data.Procedures;
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
		/// The username of the internal secure user.
		/// </summary>
		/// <remarks>
		/// The internal secure user is only used for internal highly privileged 
		/// operations. This user is given full privs to everything and is used to 
		/// manage the system tables, for authentication, etc.
		/// </remarks>
		public const String InternalSecureUsername = "@SYSTEM";

		/// <summary>
		/// The name of the default schema.
		/// </summary>
		public const String DefaultSchema = "APP";

		/// <summary>
		/// The TableDataConglomerate that contains the conglomerate of tables for
		/// this database.
		/// </summary>
		private readonly TableDataConglomerate conglomerate;

		/// <summary>
		/// An internal secure User that is given full grant access to the entire
		/// database.  This user is used to execute system level queries such as
		/// creating and updating system tables.
		/// </summary>
		private readonly User internalSystemUser;

		/// <summary>
		/// The name of this database.
		/// </summary>
		private readonly String name;

		/// <summary>
		/// A table that has a single row but no columns.
		/// </summary>
		private readonly Table singleRowTable;

		/// <summary>
		/// The DatabaseSystem that this database is part of.
		/// </summary>
		private readonly DatabaseContext context;

		/// <summary>
		/// This log file records the SQL commands executed on the server.
		/// </summary>
		private Log commandsLog;

		/// <summary>
		/// A flag which, when set to true, will cause the engine to delete the
		/// database from the file system when it is shut down.
		/// </summary>
		private bool deleteOnShutdown;

		/// <summary>
		/// This is set to true when the 'init()' method is first called.
		/// </summary>
		private bool initialised;

		///<summary>
		///</summary>
		///<param name="context"></param>
		///<param name="name"></param>
		public Database(DatabaseContext context, string name) {
			this.context = context;
			deleteOnShutdown = false;
			this.name = name;
			context.RegisterDatabase(this);
			conglomerate = new TableDataConglomerate(context, name, context.StoreSystem);
			internalSystemUser = new User(InternalSecureUsername, this, "", DateTime.Now);

			// Create the single row table
			TemporaryTable t = new TemporaryTable(this,"SINGLE_ROW_TABLE", new DataColumnInfo[0]);
			t.NewRow();
			singleRowTable = t;
		}

		~Database() {
			Dispose(false);
		}

		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Returns true if this database is in read-only mode.
		/// </summary>
		public bool IsReadOnly {
			get { return Context.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the internal system user for this database.
		/// </summary>
		private User InternalSystemUser {
			get { return internalSystemUser; }
		}

		// ---------- Log accesses ----------

		/// <summary>
		/// Returns the log file where commands are recorded.
		/// </summary>
		public Log CommandsLog {
			get { return commandsLog; }
		}

		/// <summary>
		/// Returns the conglomerate for this database.
		/// </summary>
		internal TableDataConglomerate Conglomerate {
			get { return conglomerate; }
		}

		/// <summary>
		/// Gets <b>true</b> if the database exists.
		/// </summary>
		/// <remarks>
		/// The test should be called before <see cref="Init"/> method to check
		/// if the database already exists.
		/// </remarks>
		public bool Exists {
			get {
				if (initialised)
					throw new Exception("The database is initialised, so no point testing it's existance.");

				try {
					// HACK: If the legacy style '.sf' state file exists then we must return
					//   true here because technically the database exists but is not in the
					//   correct version.
					if (conglomerate.Exists())
						return true;

					if (context.StoreSystem.StorageType == StorageType.File &&
					    File.Exists(Path.Combine(context.DatabasePath, Name + ".sf")))
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
		public bool IsInitialized {
			get { return initialised; }
		}

		/// <summary>
		/// Returns the <see cref="DatabaseContext"/> that this Database is from.
		/// </summary>
		public DatabaseContext Context {
			get { return context; }
		}

		IDatabaseContext IDatabase.Context {
			get { return Context; }
		}

		/// <summary>
		/// Returns the IStoreSystem for this Database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return context.StoreSystem; }
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
		public Table SingleRowTable {
			get { return singleRowTable; }
		}

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

			DatabaseConnection connection = new DatabaseConnection(this, user, triggerCallback);
			// Initialize the connection
			connection.Init();

			return connection;
		}


		// ---------- Schema management ----------

		private static void CreateSchemaInfoTables(DatabaseConnection connection) {
			connection.CreateSchema(DefaultSchema, "DEFAULT");
			connection.CreateSchema(InformationSchema.Name, "SYSTEM");
		}


		///<summary>
		/// Sets all the standard functions and procedures available to engine.
		///</summary>
		///<param name="connection"></param>
		///<param name="adminUser"></param>
		/// <remarks>
		/// This creates an entry in the SysFunction table for all the dynamic
		/// functions and procedures.  This may not include the functions exposed
		/// though the FunctionFactory interface.
		/// </remarks>
		public void SetupSystemFunctions(DatabaseConnection connection, string adminUser) {
			const String granter = InternalSecureUsername;

			// The manager handling the functions.
			ProcedureManager manager = connection.ProcedureManager;

			// Define the SYSTEM_MAKE_BACKUP procedure
			manager.DefineProcedure(
				new ProcedureName(SystemSchema.Name, "SYSTEM_MAKE_BACKUP"),
				"Deveel.Data.Procedure.SystemBackup.Invoke(IProcedureConnection, String)",
				PrimitiveTypes.VarString, new TType[] {PrimitiveTypes.VarString},
				adminUser);

			// -----

			// Set the grants for the procedures.
			GrantManager grants = connection.GrantManager;

			// Revoke all existing grants on the internal stored procedures.
			grants.RevokeAllGrantsOnObject(GrantObject.Table,
			                               "SYSTEM.SYSTEM_MAKE_BACKUP");

			// Grant execute priv with grant option to administrator
			grants.Grant(Privileges.ProcedureExecute,
			                GrantObject.Table,
			                "SYSTEM.SYSTEM_MAKE_BACKUP",
			                adminUser, true, granter);
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
			const string granter = InternalSecureUsername;

			// Add all priv grants to those that the system user is allowed to change
			GrantManager manager = connection.GrantManager;

			// Add schema grant for APP
			manager.Grant(Privileges.SchemaAll, GrantObject.Schema, "APP", grantee, true, granter);
			// Add public grant for SYSTEM
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, SystemSchema.Name, GrantManager.PublicUsernameStr, false, granter);
			// Add public grant for INFORMATION_SCHEMA
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, InformationSchema.Name, GrantManager.PublicUsernameStr, false, granter);

			// For all tables in the SYSTEM schema, grant all privileges to the
			// system user.
			manager.GrantToAllTablesInSchema("SYSTEM", Privileges.TableAll, grantee, false, granter);


			SystemSchema.SetTableGrants(manager, granter);
			InformationSchema.SetViewsGrants(manager, granter);
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
				conglomerate.Create();

				DatabaseConnection connection = CreateNewConnection(null, null);
				DatabaseQueryContext context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				connection.CurrentSchema = SystemSchema.Name;

				// Create the schema information tables introduced in version 0.90
				// and 0.94
				CreateSchemaInfoTables(connection);

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
				// Allow all Local connections (from within JVM).
				GrantHostAccessToUser(context, username, "Local", "%");

				// Sets the system grants for the administrator
				SetSystemGrants(connection, username);

				// Set all default system procedures.
				SetupSystemFunctions(connection, username);

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
				conglomerate.Close();
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
			if (initialised)
				throw new Exception("Init() method can only be called once.");

			// Reset all session statistics.
			Stats.ResetSession();

			try {
				string logPath = this.context.LogDirectory;
				if (logPath != null && this.context.LogQueries) {
					commandsLog = new Log(Path.Combine(logPath, "commands.log"), 256*1024, 5);
				} else {
					commandsLog = Log.Null;
				}

				// Check if the state file exists.  If it doesn't, we need to report
				// incorrect version.
				if (!StoreSystem.StoreExists(Name + "_sf")) {
					// If state store doesn't exist but the legacy style '.sf' state file
					// exists,
					if (this.context.DatabasePath != null &&
					    File.Exists(Path.Combine(this.context.DatabasePath, Name + ".sf"))) {
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
				conglomerate.Open();

				// Check the state of the conglomerate,
				DatabaseConnection connection = CreateNewConnection(null, null);
				DatabaseQueryContext context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				if (!connection.TableExists(TableDataConglomerate.PersistentVarTable)) {
					throw new DatabaseException(
						"The database_vars table doesn't exist.  This means the " +
						"database is pre-schema version 1 or the table has been deleted." +
						"If you are converting an old version of the database, please " +
						"convert the database using an older release.");
				}

				// What version is the data?
				DataTable databaseVars = connection.GetTable(TableDataConglomerate.PersistentVarTable);
				IDictionary vars = databaseVars.ToDictionary();
				String dbVersion = vars["database.version"].ToString();
				// If the version doesn't equal the current version, throw an error.
				if (!dbVersion.Equals("1.4")) {
					throw new DatabaseException(
						"Incorrect data file version '" + dbVersion + "'.  Please see " +
						"the README on how to convert the data files to the current " +
						"version.");
				}

				// Commit and close the connection.
				connection.Commit();
				connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
				connection.Close();
			} catch (TransactionException e) {
				// This would be very strange error to receive for in initializing
				// database...
				throw new ApplicationException("Transaction Error: " + e.Message);
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new ApplicationException("IO Error: " + e.Message);
			}

			initialised = true;
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
			if (initialised == false) {
				throw new ApplicationException("The database is not initialized.");
			}

			try {
				if (deleteOnShutdown) {
					// Delete the conglomerate if the database is set to delete on
					// shutdown.
					conglomerate.Delete();
				} else {
					// Otherwise close the conglomerate.
					conglomerate.Close();
				}
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new ApplicationException("IO Error: " + e.Message, e);
			}

			// Shut down the logs...
			if (commandsLog != null) {
				commandsLog.Close();
			}

			initialised = false;
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
			if (initialised == false)
				throw new ApplicationException("The database is not initialized.");

			// Set up the destination conglomerate to copy all the data to,
			// Note that this sets up a typical destination conglomerate and changes
			// the cache size and disables the debug log.
			SystemContext copyContext = new SystemContext();
			DbConfig config = new DbConfig();
			config.DatabasePath = Path.GetFullPath(path);
			config.LogPath = "";
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

			TableDataConglomerate destConglomerate = new TableDataConglomerate(copyContext, name, copyContext.StoreSystem);

			// Open the congloemrate
			destConglomerate.MinimalCreate();

			try {
				// Make a copy of this conglomerate into the destination conglomerate,
				conglomerate.LiveCopyTo(destConglomerate);
			} finally {
				// Close the congloemrate when finished.
				destConglomerate.Close();
				// Dispose the TransactionSystem
				copyContext.Dispose();
			}
		}

		// ---------- Server side procedures ----------

		///<summary>
		/// Resolves a procedure name into a <see cref="IDatabaseProcedure"/> object.
		///</summary>
		///<param name="procedureName"></param>
		///<param name="connection"></param>
		/// <remarks>
		/// This is used for finding a server side script.
		/// </remarks>
		///<returns></returns>
		///<exception cref="DatabaseException">
		/// If the procedure could not be resolved or there was an error retrieving it.
		/// </exception>
		public IDatabaseProcedure GetDbProcedure(string procedureName, DatabaseConnection connection) {
			// The procedure we are getting.
			IDatabaseProcedure procedureInstance;

			try {
				// Find the procedure
				Type proc = Type.GetType("Deveel.Data.Procedure." + procedureName);
				// Instantiate a new instance of the procedure
				procedureInstance = (IDatabaseProcedure) Activator.CreateInstance(proc);

				Logger.Info(this, "Getting raw class file: " + procedureName);
			} catch (AccessViolationException e) {
				Logger.Error(this, e);
				throw new DatabaseException("Illegal Access: " + e.Message);
			} catch (TypeInitializationException e) {
				Logger.Error(this, e);
				throw new DatabaseException("Instantiation Error: " + e.Message);
			} catch (TypeLoadException e) {
				Logger.Error(this, e);
				throw new DatabaseException("Type Not Found: " + e.Message);
			}

			// Return the procedure.
			return procedureInstance;
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