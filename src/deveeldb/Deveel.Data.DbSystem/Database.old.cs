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
using System.Data;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Protocol;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
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
	public sealed class Database : IDatabase {
		///<summary>
		///</summary>
		///<param name="context"></param>
		///<param name="name"></param>
		public Database(DatabaseContext context, string name) {
			Context = context;
			DeleteOnShutdown = false;
			Name = name;
			context.RegisterDatabase(this);
			Conglomerate = new TableDataConglomerate(context, name, context.StoreSystem);
			InternalSystemUser = new User(User.SystemName, this, "", DateTime.Now);

			UserManager = new UserManager(this);

			// Create the single row table
			var t = new TemporaryTable(this,"SINGLE_ROW_TABLE", new DataColumnInfo[0]);
			t.NewRow();
			SingleRowTable = t;

			Version = GetDatabaseVersion();
			DataVersion = GetDataVersion();
		}

		~Database() {
			Dispose(false);
		}

		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		public string Name { get; private set; }

		public Version Version { get; private set; }

		private Version DataVersion { get; set; }

		public string DefaultSchema {
			get { return Context.Config.DefaultSchema(); }
		}

		/// <summary>
		/// Returns the internal system user for this database.
		/// </summary>
		private User InternalSystemUser { get; set; }

		public UserManager UserManager { get; private set; }


		/// <summary>
		/// Returns the conglomerate for this database.
		/// </summary>
		public TableDataConglomerate Conglomerate { get; private set; }

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
					//throw new Exception("The database is initialised, so no point testing it's existance.");
					return true;

				try {
					return Conglomerate.Exists();
				} catch (IOException e) {
					Context.Logger.Error(this, String.Format("Could not assess the existence of the database {0}", Name));
					Context.Logger.Error(this, e);
					throw new DatabaseException("An error occurred while testing database existance.", e);
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

		public string StateResourceName {
			get { return String.Format("{0}_sf", Name); }
		}

		IDatabaseContext IDatabase.Context {
			get { return Context; }
		}

		/// <summary>
		/// Returns a static table that has a single row but no columns.
		/// </summary>
		/// <remarks>
		/// This table is useful for certain database operations.
		/// </remarks>
		public Table SingleRowTable { get; private set; }

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
		public IDatabaseConnection CreateNewConnection(User user, TriggerCallback triggerCallback) {
			if (user == null)
				user = InternalSystemUser;

			var connection = new DatabaseConnection(this, user, triggerCallback);
			// Initialize the connection
			connection.Init();

			return connection;
		}


		// ---------- Schema management ----------

		private void CreateSchemata(IDatabaseConnection connection) {
			try {
				connection.CreateSchema(ConfigDefaultValues.DefaultSchema, SchemaTypes.Default);
				connection.CreateSchema(InformationSchema.Name, SchemaTypes.System);
			} catch (DatabaseException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseException("Unble to create system schemata.", ex);
			}
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
		private void SetSystemGrants(IDatabaseConnection connection, string grantee) {
			try {
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
			} catch (DatabaseSecurityException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSecurityException(String.Format("An error occurred while assigning system grants to {0}", grantee), ex);
			}
		}

		private const string DataVersionFileName = "Deveel.Data.DataVersion";

		private Version GetDatabaseVersion() {
			return typeof (Database).Assembly.GetName().Version;
		}

		private Version GetDataVersion() {
			//TODO: Read the embedded file for the real version ...
			return new Version(1,1);
		}

		private void AssertDataVersion() {
			try {
				var dataVersion = DataVersion;

				// Check the state of the conglomerate,
				IDatabaseConnection connection = CreateNewConnection(null, null);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				if (!connection.TableExists(SystemSchema.PersistentVarTable)) {
					throw new DatabaseConfigurationException(
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
			} catch (DatabaseException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("An error occurred while assessing the database version.", ex);
			}
		}

		private void SetCurrentDataVersion(IDatabaseConnection transaction) {
			try {
				var dataVersion = DataVersion;

				// Insert the version number of the database
				transaction.SetPersistentVariable("database.version", dataVersion.ToString(2));
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("An error occurred while setting the database version into the system.", ex);
			}
		}

		private void CreateAdminUser(DatabaseQueryContext context, string username, string password) {
			try {
				// Creates the administrator user.
				UserManager.CreateUser(context, username, password);
				// This is the admin user so add to the 'secure access' table.
				UserManager.AddUserToGroup(context, username, SystemGroupNames.SecureGroup);
				// Allow all localhost TCP connections.
				// NOTE: Permissive initial security!
				UserManager.GrantHostAccessToUser(context, username, KnownConnectionProtocols.TcpIp, "%");
				// Allow all Local connections.
				UserManager.GrantHostAccessToUser(context, username, KnownConnectionProtocols.Local, "%");

				// Sets the system grants for the administrator
				SetSystemGrants(context.Connection, username);
			} catch (DatabaseSecurityException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSecurityException("Could not create the administrator user for the database.", ex);
			}
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
			if (Context.IsReadOnly)
				throw new DatabaseException("Can not create database in Read only mode.");

			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");
			if (String.IsNullOrEmpty(password))
				throw new ArgumentNullException("password");

			try {
				// Create the conglomerate
				Conglomerate.Create();

				var connection = CreateNewConnection(null, null);
				connection.AutoCommit = false;

				var context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				connection.CurrentSchema = SystemSchema.Name;

				// Create the schema information tables
				CreateSchemata(connection);

				// The system tables that are present in every conglomerate.
				SystemSchema.CreateTables(connection);

				// Create the system views
				InformationSchema.CreateSystemViews(connection);

				CreateAdminUser(context, username, password);

				SetCurrentDataVersion(connection);

				// Set all default system procedures.
				SystemSchema.SetupSystemFunctions(connection, username);

				try {
					// Close and commit this transaction.
					connection.Commit();
				} catch (TransactionException e) {
					Context.Logger.Error(this, e);
					throw new DatabaseException("Could not commit the initial information", e);
				}

				connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
				connection.Close();

				// Close the conglomerate.
				Conglomerate.Close();
			} catch (DatabaseException e) {
				Context.Logger.Error(this, e);
				throw;
			} catch (Exception e) {
				Context.Logger.Error(this, e);
				throw new DatabaseException("An error occurred while creating the database.", e);
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
				throw new DatabaseException("The database was already initialized.");

			// Reset all session statistics.
			Context.Stats.ResetSession();

			try {
				// Check if the state file exists.  If it doesn't, we need to report
				// incorrect version.
				if (!Context.StoreSystem.StoreExists(StateResourceName))
					// If neither store or state file exist, assume database doesn't
					// exist.
					throw new DatabaseException(String.Format("The database {0} does not exist.", Name));

				// Open the conglomerate
				Conglomerate.Open();

				AssertDataVersion();
			} catch (DatabaseException e) {
				throw;
			} catch (Exception e) {
				throw new DatabaseException("An error occurred when initializing the database.", e);
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
		/// If <see cref="DeleteOnShutdown"/> is true, the database will delete itself from the file 
		/// system when it shuts down.
		/// </para>
		/// </remarks>
		public void Shutdown() {
			if (IsInitialized == false)
				throw new DataException("The database is not initialized.");

			try {
				if (DeleteOnShutdown) {
					// Delete the conglomerate if the database is set to delete on
					// shutdown.
					Conglomerate.Delete();
				} else {
					// Otherwise close the conglomerate.
					Conglomerate.Close();
				}
			} catch (DatabaseException e) {
				Context.Logger.Error(this, "Error while executing the database shutdown");
				Context.Logger.Error(this, e);
				throw;
			} catch (Exception e) {
				Context.Logger.Error(this, "Error while executing the database shutdown");
				Context.Logger.Error(this, e);
				throw new DatabaseException("An error occurred during database shutdown.", e);
			} finally {
				IsInitialized = false;
			}
		}

		/// <summary>
		///  If the 'delete_on_shutdown' flag is set, the database will delete 
		///  the database from the file system when it is shutdown.
		/// </summary>
		/// <value></value>
		/// <remarks>
		///  <b>Note</b>: Use with care - if this is set to true and the database is 
		///  shutdown it will result in total loss of data.
		///  </remarks>
		public bool DeleteOnShutdown { get; set; }

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
				throw new DataException("The database is not initialized.");

			// Set up the destination conglomerate to copy all the data to,
			// Note that this sets up a typical destination conglomerate and changes
			// the cache size and disables the debug log.
			var copyContext = new SystemContext();
			var config = DbConfig.Default;
			config.DatabasePath(Path.GetFullPath(path));
			config.LogPath("");
			config.LogLevel(LogLevel.Error);
			// Set data cache to 1MB
			config.DataCacheSize(1024 * 1000);
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

		#region Implementation of IDisposable

		private void Dispose(bool disposing) {
			if (disposing) {
				if (IsInitialized)
					Console.Error.WriteLine("Database object was finalized and is initialized!");

				Conglomerate.Dispose();
				Context.Dispose();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#endregion
	}
}