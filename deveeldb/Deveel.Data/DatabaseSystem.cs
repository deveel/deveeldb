//  
//  DatabaseSystem.cs
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
using System.Threading;

using Deveel.Data.Caching;
using Deveel.Data.Control;
using Deveel.Data.Sql;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// Provides information about shared resources available for the entire 
	/// database system running in the current environment.
	/// </summary>
	/// <remarks>
	/// Shared information includes configuration details, <see cref="DataCellCache"/>, 
	/// plug-ins, user management, etc.
	/// </remarks>
	public sealed class DatabaseSystem : TransactionSystem {
		private readonly ArrayList shut_down_delegates = new ArrayList();

		/// <summary>
		/// The list of Database objects that this system is being managed by this environment.
		/// </summary>
		private ArrayList database_list;

		/// <summary>
		/// True if all queries on the database should be logged in the 'commands.log'
		/// file in the log directory.
		/// </summary>
		private bool query_logging;

		/// <summary>
		/// Set to true when the database is shut down.
		/// </summary>
		private bool shutdown = false;

		/// <summary>
		/// The thread to run to shut down the database system.
		/// </summary>
		private ShutdownThread shutdown_thread;

		/// <summary>
		/// The StatementCache that maintains a cache of parsed queries.
		/// </summary>
		private StatementCache statement_cache = null;

		/// <summary>
		/// The UserManager object that handles users connected to the database engine.
		/// </summary>
		private UserManager user_manager;

		/// <summary>
		/// The WorkerPool object that manages access to the database(s) in the system.
		/// </summary>
		private WorkerPool worker_pool;


		// ---------- Queries ----------

		/// <summary>
		/// If query logging is enabled (all queries are output to 'commands.log' in
		/// the log directory), this returns true.  Otherwise it returns false.
		/// </summary>
		public bool LogQueries {
			get { return query_logging; }
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
		public StatementCache StatementCache {
			get { return statement_cache; }
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
		internal UserManager UserManager {
			get { return user_manager; }
		}

		/// <summary>
		/// Returns true if <see cref="StartShutDownThread"/> method has been 
		/// called.
		/// </summary>
		internal bool HasShutDown {
			get { return shutdown; }
		}

		/// <inheritdoc/>
		public override void Init(IDbConfig config) {
			base.Init(config);

			database_list = new ArrayList();

			// Create the user manager.
			user_manager = new UserManager();

			if (config != null) {
				// Set up the statement cache.
				bool status = GetConfigBoolean("statement_cache", true);
				if (status) {
					statement_cache = new StatementCache(this, 127, 140, 20);
				}
				Debug.Write(DebugLevel.Message, typeof (DatabaseSystem), "statement_cache = " + status);

				// The maximum number of worker threads.
				int max_worker_threads = GetConfigInt("maximum_worker_threads", 4);
				if (max_worker_threads <= 0) {
					max_worker_threads = 1;
				}
				Debug.Write(DebugLevel.Message, typeof (DatabaseSystem), "Max worker threads set to: " + max_worker_threads);
				worker_pool = new WorkerPool(this, max_worker_threads);

				// Should we be logging commands?
				query_logging = GetConfigBoolean("query_logging", false);
			} else {
				throw new ApplicationException("Config bundle already set.");
			}

			shutdown = false;
		}

		/// <inheritdoc/>
		public override void Dispose() {
			base.Dispose();
			worker_pool = null;
			database_list = null;
			user_manager = null;
		}

		/// <summary>
		/// Given a <see cref="Transaction.CheckExpression">check expression</see>, this 
		/// will prepare the expression and return a new prepared <see cref="Transaction.CheckExpression"/>.
		/// </summary>
		/// <remarks>
		/// A <see cref="DatabaseSystem"/> resolves the variables (ignoring case if 
		/// necessary) and the functions of the expression.
		/// </remarks>
		public override Transaction.CheckExpression PrepareTransactionCheckConstraint(DataTableDef table_def,
		                                                                              Transaction.CheckExpression check) {
			return base.PrepareTransactionCheckConstraint(table_def, check);
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
			worker_pool.WaitUntilAllWorkersQuiet();
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
			worker_pool.SetIsExecutingCommands(status);
		}

		/// <summary>
		/// Executes database functions from the 'run' method of the given 
		/// runnable instance on the first available worker thread.
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
		internal void Execute(User user, DatabaseConnection database, IDatabaseEvent runner) {
			worker_pool.Execute(user, database, runner);
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
		internal void RegisterShutDownDelegate(IDatabaseEvent value) {
			shut_down_delegates.Add(value);
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
				shutdown_thread = new ShutdownThread(this);
				shutdown_thread.Start();
			}
		}

		/// <summary>
		/// Wait until the shutdown thread has completed. (Shutdown process
		/// has finished).
		/// </summary>
		internal void WaitUntilShutdown() {
			shutdown_thread.WaitTillFinished();
		}

		internal void RegisterDatabase(Database database) {
			lock (this) {
				if (database_list == null)
					database_list = new ArrayList();
				if (database_list.Contains(database))
					throw new DatabaseException("The database '" + database.Name + "' is already registered.");

				database_list.Add(database);
			}
		}

		#region Nested type: ShutdownThread

		/// <summary>
		/// The shut down thread.  Started when 'shutDown' is called.
		/// </summary>
		private class ShutdownThread {
			private readonly Thread thread;
			private DatabaseSystem ds;
			private bool finished = false;

			internal ShutdownThread(DatabaseSystem ds) {
				this.ds = ds;
				thread = new Thread(new ThreadStart(run));
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

			private void run() {
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
				ds.worker_pool.Shutdown();

				int sz = ds.shut_down_delegates.Count;
				if (sz == 0) {
					ds.Debug.Write(DebugLevel.Warning, this, "No shut down delegates registered!");
				} else {
					for (int i = 0; i < sz; ++i) {
						IDatabaseEvent shut_down_delegate = (IDatabaseEvent) ds.shut_down_delegates[i];
						if (shut_down_delegate != null)
							// Run the shut down delegates
							shut_down_delegate.Execute();
					}
					ds.shut_down_delegates.Clear();
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