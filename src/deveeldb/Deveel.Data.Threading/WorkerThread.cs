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
using System.Threading;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Threading {
	/// <summary>
	/// This is a worker thread.  This is given commands to execute by the
	/// WorkerPool.
	/// </summary>
	sealed class WorkerThread {
		private readonly Thread thread;
		/// <summary>
		/// If this is set to true, the server displays the time each executed
		/// command took.
		/// </summary>
		private const bool DISPLAY_COMMAND_TIME = false;

		/// <summary>
		/// Set to true to turn off this worker thread.
		/// </summary>
		private bool shutdown;
		/// <summary>
		/// The command we are currently processing.
		/// </summary>
		private EventHandler command;
		/// <summary>
		/// The time the command was started.
		/// </summary>
		private DateTime start_time;
		/// <summary>
		/// The WorkerPool object that this worker thread is for.
		/// </summary>
		private readonly WorkerPool worker_pool;

		internal const string THREAD_NAME = "Worker";

		/// <summary>
		/// Constructs the thread.
		/// </summary>
		/// <param name="worker_pool"></param>
		public WorkerThread(WorkerPool worker_pool) {
			thread = new Thread(new ThreadStart(Run));
			// thread.IsBackground = true;
			thread.Name = THREAD_NAME;
			this.worker_pool = worker_pool;
			command = null;
			shutdown = false;
		}

		// ---------- Other methods ----------

		/// <summary>
		/// Shuts down this worker thread.
		/// </summary>
		internal void Shutdown() {
			lock (this) {
				shutdown = true;
				Monitor.PulseAll(this);
			}
		}

		/// <summary>
		/// Tells the worker thread that the user is executing the given command.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database_connection"></param>
		/// <param name="runner"></param>
		internal void Execute(User user, DatabaseConnection database_connection, EventHandler runner) {
			// This should help to prevent deadlock
			lock (this) {
				if (command == null) {
					command = runner;
					Monitor.PulseAll(this);
				} else {
					throw new Exception("Deadlock Error, tried to execute command on running worker.");
				}
			}
		}

		/// <summary>
		/// Starts executing this worker thread.
		/// </summary>
		private void Run() {
			lock (this) {
				while (true) {
					try {
						// Is there any command waiting to be executed?
						if (command != null) {
							try {
								// Record the time this command was started.
								start_time = DateTime.Now;
								// Run the command
								command(this, EventArgs.Empty);
							} finally {
								command = null;
								// Record the time the command ended.
								TimeSpan elapsed_time = DateTime.Now - start_time;
								if (DISPLAY_COMMAND_TIME) {
									Console.Error.Write("[Worker] Completed command in ");
									Console.Error.Write(elapsed_time);
									Console.Error.Write(" ms.  ");
									Console.Error.WriteLine(this);
								}
							}
						}

						// Notifies the thread pool manager that this worker is ready
						// to go.
						worker_pool.OnWorkerReady(this);
						// NOTE: The above command may cause a command to be posted on this
						//   worker.
						while (command == null) {
							try {
								// Wait until there is a new command to process.
								Monitor.Wait(this);
							} catch (ThreadInterruptedException e) {
								/* ignore */
							}
							// Shut down if we need to...
							if (shutdown) {
								return;
							}
						}

					} catch (Exception e) {
						worker_pool.Logger.Error(this, "Worker thread interrupted because of exception:\n" + e.Message);
						worker_pool.Logger.Error(this, e);
					}
				}
			}
		}

		public void Start() {
			thread.Start();
		}
	}
}