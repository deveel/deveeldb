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
using System.Threading;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data.Threading {
	/// <summary>
	/// Maintains a pool of worker threads that are used to dispatch commands 
	/// to a <see cref="Database"/> sub-system.
	/// </summary>
	sealed class WorkerPool {
		/// <summary>
		/// The TransactionSystem that this pool is part of.
		/// </summary>
		internal readonly SystemContext context;
		/// <summary>
		/// This is the maximum number of worker threads that will be created.
		/// </summary>
		private int MAXIMUM_WORKER_THREADS = 4;
		/// <summary>
		/// This is a queue of 'WorkerThread' objects that are currently available
		/// to process commands from the service providers.
		/// </summary>
		private readonly ArrayList available_worker_threads;
		/// <summary>
		/// The number of worker threads that have been created in total.
		/// </summary>
		private int worker_thread_count;
		/// <summary>
		/// A list of pending event objects that are due to be executed.  This is
		/// a queue of events to be run.
		/// </summary>
		private readonly ArrayList run_queue;
		/// <summary>
		/// If this is set to false, then no commands will be executed by the
		/// <see cref="Execute"/> method.
		/// </summary>
		private bool is_executing_commands;


		internal WorkerPool(SystemContext context, int max_worker_threads) {
			this.context = context;
			MAXIMUM_WORKER_THREADS = max_worker_threads;

			is_executing_commands = false;

			// Set up the run queue
			run_queue = new ArrayList();
			// Set up the worker threads
			available_worker_threads = new ArrayList();
			worker_thread_count = 0;
			//    // Create a single worker thread and start it.
			//    ++worker_thread_count;
			//    WorkerThread wt = new WorkerThread(this);
			//    wt.start();

		}


		// ---------- Thread Pooling methods ----------

		internal ILogger Logger {
			get { return context.Logger; }
		}

		/// <summary>
		/// This is called by a WorkerThread when it is decided that it is ready
		/// to service a new command.
		/// </summary>
		/// <param name="worker_thread"></param>
		internal void OnWorkerReady(WorkerThread worker_thread) {
			lock (available_worker_threads) {
				// Add it to the queue of worker threads that are available.
				available_worker_threads.Add(worker_thread);

				// Are there any commands pending?
				int q_len = run_queue.Count;
				if (q_len > 0) {
					// Execute the bottom element on the queue
					RunCommand command = (RunCommand)run_queue[0];
					run_queue.RemoveAt(0);
					Execute(command.user, command.database, command.runnable);
				}
			}
		}

		/// <summary>
		/// This returns the first available WorkerThread object from the 
		/// thread pool.
		/// </summary>
		/// <remarks>
		/// If there are no available worker threads available then it returns
		/// null. This method must execute fast and must not block.
		/// </remarks>
		private WorkerThread FirstWaitingThread {
			get {
				lock (available_worker_threads) {
					// Is there a worker thread available?
					int size = available_worker_threads.Count;
					if (size > 0) {
						// Yes so remove the first element and return it.
						WorkerThread wt = (WorkerThread) available_worker_threads[0];
						available_worker_threads.RemoveAt(0);
						return wt;
					} else {
						// Otherwise create a new worker thread if we can.
						if (worker_thread_count < MAXIMUM_WORKER_THREADS) {
							++worker_thread_count;
							WorkerThread wt = new WorkerThread(this);
							wt.Start();
							// NOTE: We must _not_ return the worker thread we have just created.
							//   We must wait until the worker thread has made it self known by
							//   it calling the 'OnWorkerReady' method.
						}
						return null;
					}
				}
			}
		}

		/// <summary>
		/// Executes database functions from the 'run' method of the given 
		/// runnable instance on a worker thread.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database"></param>
		/// <param name="runner"></param>
		/// <remarks>
		/// All database functions should go through a worker thread. If we 
		/// ensure this, we can easily stop all database functions from 
		/// executing. Also, we only need to have a certain number of threads 
		/// active at any one time rather than a unique thread for each 
		/// connection.
		/// </remarks>
		public void Execute(User user, DatabaseConnection database, EventHandler runner) {
			lock (available_worker_threads) {
				if (is_executing_commands) {
					WorkerThread worker = FirstWaitingThread;
					if (worker != null) {
						//          Console.Out.WriteLine("[Database] executing runner");
						worker.Execute(user, database, runner);
						return;
					}
				}
				//      Console.Out.WriteLine("[Database] adding to run queue");
				RunCommand command = new RunCommand(user, database, runner);
				run_queue.Add(command);
			}
		}

		/// <summary>
		/// Controls whether the database is allowed to execute commands or not.
		/// </summary>
		/// <value></value>
		/// <remarks>
		/// If this is set to true, then calls to <see cref="Execute"/> will make 
		/// commands execute.
		/// </remarks>
		public bool IsExecutingCommands {
			get { return is_executing_commands; }
			set {
				lock (available_worker_threads) {
					if (value == true) {
						is_executing_commands = true;

						// Execute everything on the queue
						for (int i = run_queue.Count - 1; i >= 0; --i) {
							RunCommand command = (RunCommand) run_queue[i];
							run_queue.RemoveAt(i);
							Execute(command.user, command.database, command.runnable);
						}
					} else
						is_executing_commands = false;
				}
			}
		}

		/// <summary>
		/// Waits until all executing commands have stopped.
		/// </summary>
		/// <remarks>
		/// This is best called right after a call to <see cref="SetIsExecutingCommands"/> 
		/// to <b>false</b>. If these two commands are run, the database is 
		/// in a known state where no commands can be executed.
		/// </para>
		/// <para>
		/// <b>Note</b> This can't be called from the <see cref="WorkerThread"/>.
		/// Deadlock will result if we were allowed to do this.
		/// </para>
		/// </remarks>
		public void WaitUntilAllWorkersQuiet() {
			if (Thread.CurrentThread.Name == WorkerThread.THREAD_NAME) {
				throw new ApplicationException("Can't call this method from a WorkerThread!");
			}

			lock (available_worker_threads) {
				// loop until available works = total worker thread count.
				while (worker_thread_count != available_worker_threads.Count) {
					// Wait half a second
					try {
						Monitor.Wait(available_worker_threads, 500);
					} catch (ThreadInterruptedException e) { }
					// ISSUE: If this lasts for more than 10 minutes, one of the worker
					//   threads is likely in a state of deadlock.  If this happens, we
					//   should probably find all the open worker threads and clean them
					//   up nicely.
				}
			}
		}

		/// <summary>
		/// Shuts down the <see cref="WorkerPool"/> object stopping all worker 
		/// threads.
		/// </summary>
		public void Shutdown() {
			lock (available_worker_threads) {
				while (available_worker_threads.Count > 0) {
					WorkerThread wt = (WorkerThread)available_worker_threads[0];
					available_worker_threads.RemoveAt(0);
					--worker_thread_count;
					wt.Shutdown();
				}
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// Structures within the run_queue list.
		/// </summary>
		/// <remarks>
		/// This stores the Runnable to run and the User that's executing 
		/// the command.
		/// </remarks>
		private sealed class RunCommand {
			public readonly User user;
			public readonly DatabaseConnection database;
			public readonly EventHandler runnable;

			public RunCommand(User user, DatabaseConnection database, EventHandler runnable) {
				this.user = user;
				this.database = database;
				this.runnable = runnable;
			}
		}
	}
}