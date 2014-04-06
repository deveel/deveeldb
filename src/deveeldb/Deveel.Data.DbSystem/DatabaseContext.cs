// 
//  Copyright 2010  Deveel
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
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Provides information about shared resources available for the entire 
	/// database system running in the current environment.
	/// </summary>
	/// <remarks>
	/// Shared information includes configuration details, <see cref="DataCellCache"/>, 
	/// plug-ins, user management, etc.
	/// </remarks>
	public sealed class DatabaseContext : SystemContext, IDatabaseContext {
		private EventHandler shutdownCallback;

		/// <summary>
		/// The list of Database objects that this system is being managed by this environment.
		/// </summary>
		private List<Database> databaseList;

		/// <summary>
		/// True if all queries on the database should be logged in the 'commands.log'
		/// file in the log directory.
		/// </summary>
		private bool queryLogging;

		/// <summary>
		/// Set to true when the database is shut down.
		/// </summary>
		private bool shutdown;

		/// <summary>
		/// The thread to run to shut down the database system.
		/// </summary>
		private ShutdownThread shutdownThread;

		/// <summary>
		/// The StatementCache that maintains a cache of parsed queries.
		/// </summary>
		private StatementCache statementCache;

		/// <summary>
		/// The UserManager object that handles users connected to the database engine.
		/// </summary>
		private UserManager userManager;

		/// <summary>
		/// The WorkerPool object that manages access to the database(s) in the system.
		/// </summary>
		private WorkerPool workerPool;


		// ---------- Queries ----------

		/// <summary>
		/// If query logging is enabled (all queries are output to 'commands.log' in
		/// the log directory), this returns true.  Otherwise it returns false.
		/// </summary>
		public bool LogQueries {
			get { return queryLogging; }
		}


		/// <summary>
		/// Returns the StatementCache that is used to cache StatementTree objects
		/// that are being queried by the database. 
		/// </summary>
		/// <remarks>
		/// This is used to reduce the SQL command parsing overhead.
		/// <para>
		/// If this method returns 'null' then statement caching is disabled.
		/// </para>
		/// </remarks>
		internal StatementCache StatementCache {
			get { return statementCache; }
		}


		/// <summary>
		/// Returns the <see cref="UserManager"/> object that handles 
		/// users that are connected to the database.
		/// </summary>
		/// <remarks>
		/// The aim of this class is to unify the way users are handled 
		/// by the engine.  It allows us to perform queries to see who's
		/// connected, and any inter-user communication (triggers).
		/// </remarks>
		public UserManager UserManager {
			get { return userManager; }
		}

		/// <summary>
		/// Returns true if <see cref="StartShutDownThread"/> method has been 
		/// called.
		/// </summary>
		internal bool HasShutDown {
			get { return shutdown; }
		}

		/// <inheritdoc/>
		public override void Init(DbConfig config) {
			base.Init(config);

			databaseList = new List<Database>();

			// Create the user manager.
			userManager = new UserManager();

			if (config != null) {
				// Set up the statement cache.
				if (config.GetValue(ConfigKeys.CacheStatements, true)) {
					statementCache = new StatementCache(this, 127, 140, 20);
					Logger.Message(this, "statement cache ENABLED");
				} else {
					Logger.Message(this, "statement cache DISABLED");
				}

				// The maximum number of worker threads.
				int maxWorkerThreads = config.GetValue(ConfigKeys.MaxWorkerThreads, 4);
				if (maxWorkerThreads <= 0)
					maxWorkerThreads = 1;

				Logger.Message(this, "Max worker threads set to: " + maxWorkerThreads);
				workerPool = new WorkerPool(this, maxWorkerThreads);

				// Should we be logging commands?
				queryLogging = config.GetValue(ConfigKeys.LogQueries, false);
			} else {
				throw new ApplicationException("Config bundle already set.");
			}

			shutdown = false;
		}

		/// <inheritdoc/>
		protected override void Dispose(bool disposing) {
			if (disposing) {
				workerPool = null;
				databaseList = null;
				userManager = null;
			}

			base.Dispose(disposing);
		}

		/// <summary>
		/// Waits until all executing commands have stopped.
		/// </summary>
		/// <remarks>
		/// This is best called right after a call to 'setIsExecutingCommands(false)'. 
		/// If these two commands are run, the database is in a known state where 
		/// no commands can be executed.
		/// <para>
		/// <b>Note</b>: This can't be called from the WorkerThread. Deadlock 
		/// will result if we were allowed to do this.
		/// </para>
		/// </remarks>
		internal void WaitUntilAllWorkersQuiet() {
			workerPool.WaitUntilAllWorkersQuiet();
		}

		/// <summary>
		/// Controls whether the database system is allowed to execute commands or 
		/// not.
		/// </summary>
		/// <remarks>
		/// If this is set to true, then calls to <see cref="Execute"/> will 
		/// be executed as soon as there is a free worker thread available.
		/// Otherwise no commands are executed until this is enabled.
		/// </remarks>
		internal void SetIsExecutingCommands(bool status) {
			workerPool.SetIsExecutingCommands(status);
		}

		/// <summary>
		/// Executes the given delegate on the first available worker thread.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database"></param>
		/// <param name="runner"></param>
		/// <remarks>
		/// All database functions must go through a worker thread. If we 
		/// ensure this, we can easily stop all database functions from 
		/// executing if need be.  Also, we only need to have a certain number 
		/// of threads active at any one time rather than a unique thread for 
		/// each connection.
		/// </remarks>
		internal void Execute(User user, DatabaseConnection database, EventHandler runner) {
			workerPool.Execute(user, database, runner);
		}

		// ---------- Shut down methods ----------

		/// <summary>
		/// Registers the delegate that is executed when the shutdown thread
		/// is activated.
		/// </summary>
		/// <remarks>
		/// Only one delegate may be registered with the database
		/// system.  This is only called once and shuts down the relevant
		/// database services.
		/// </remarks>
		internal void RegisterShutDownDelegate(EventHandler callback) {
			shutdownCallback = (EventHandler) Delegate.Combine(shutdownCallback, callback);
		}

		/// <summary>
		/// This starts the shut-down thread that is used to shut down the 
		/// database server.
		/// </summary>
		/// <remarks>
		/// Since the actual shutdown method is dependent on the type of
		/// database we are running (server or stand-alone) we delegate the
		/// shutdown method to the registered shutdown delegate.
		/// </remarks>
		internal void StartShutDownThread() {
			if (!shutdown) {
				shutdown = true;
				shutdownThread = new ShutdownThread(this);
				shutdownThread.Start();
			}
		}

		/// <summary>
		/// Wait until the shutdown thread has completed. (Shutdown process
		/// has finished).
		/// </summary>
		internal void WaitUntilShutdown() {
			shutdownThread.WaitTillFinished();
		}

		void IDatabaseContext.RegisterDatabase(IDatabase database) {
			RegisterDatabase((Database)database);
		}

		internal void RegisterDatabase(Database database) {
			lock (this) {
				if (databaseList == null)
					databaseList = new List<Database>();
				if (databaseList.Contains(database))
					throw new DatabaseException("The database '" + database.Name + "' is already registered.");

				databaseList.Add(database);
			}
		}

		IDatabase IDatabaseContext.GetDatabase(string name) {
			throw new NotImplementedException();
		}

		#region Nested type: ShutdownThread

		/// <summary>
		/// The shut down thread.  Started when 'shutDown' is called.
		/// </summary>
		private class ShutdownThread {
			private readonly Thread thread;
			private readonly DatabaseContext ds;
			private bool finished;

			internal ShutdownThread(DatabaseContext ds) {
				this.ds = ds;
				thread = new Thread(Run);
				thread.Name = "Shutdown Thread";
			}

			internal void WaitTillFinished() {
				lock (this) {
					while (finished == false) {
						try {
							Monitor.Wait(this);
						} catch (ThreadInterruptedException) {
						}
					}
				}
			}

			private void Run() {
				lock (this) {
					if (finished) {
						return;
					}
				}

				// We need this pause so that the command that executed this shutdown
				// has time to exit and retrieve the single row result.
				try {
					Thread.Sleep(1500);
				} catch (ThreadInterruptedException) {
				}
				// Stops commands from being executed by the system...
				ds.SetIsExecutingCommands(false);
				// Wait until the worker threads are all quiet...
				ds.WaitUntilAllWorkersQuiet();

				// Close the worker pool
				ds.workerPool.Shutdown();

				EventHandler callback = ds.shutdownCallback;
				if (callback == null) {
					ds.Logger.Warning(this, "No shut down callbacks registered!");
				} else {
					callback(this, EventArgs.Empty);
					ds.shutdownCallback = null;
				}

				lock (this) {
					// Wipe all variables from this object
					ds.Dispose();

					finished = true;
					Monitor.PulseAll(this);
				}
			}

			public void Start() {
				thread.Start();
			}
		} ;

		#endregion
	}
}