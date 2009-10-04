//  
//  WorkerThread.cs
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
using System.Threading;

using Deveel.Diagnostics;

namespace Deveel.Data {
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
			thread = new Thread(new ThreadStart(run));
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
		private void run() {
			lock (this) {
				while (true) {
					try {
						// Is there any command waiting to be executed?
						if (command != null) {
							try {
								// Record the time this command was started.
								start_time = DateTime.Now;
								// Run the command
								command(worker_pool.system, EventArgs.Empty);
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
						Debug.Write(DebugLevel.Error, this,
									  "Worker thread interrupted because of exception:\n" +
									  e.Message);
						Debug.WriteException(e);
					}
				}
			}
		}

		public void Start() {
			thread.Start();
		}
	}
}