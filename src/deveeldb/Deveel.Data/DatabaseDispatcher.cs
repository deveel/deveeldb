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
using System.Collections;
using System.Threading;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This is the database system dispatcher thread.
	/// </summary>
	/// <remarks>
	/// This is a thread that runs in the background servicing delayed events. 
	/// This thread serves a number of purposes. It can be used to perform 
	/// optimizations/clean ups in the background (similar to hotspot).  It could 
	/// be used to pause until sufficient information has been collected or there 
	/// is a lull in work before it does a query in the background.  For example, 
	/// if a VIEW is invalidated because the underlying data changes, then we can 
	/// wait until the data has finished updating, then perform the view query to
	/// update it correctly.
	/// </remarks>
	class DatabaseDispatcher {
		private readonly Thread thread;
		private readonly ArrayList event_queue = new ArrayList();

		private TransactionSystem system;

		private bool finished;

		/**
		 * NOTE: Constructing this object will start the thread.
		 */
		internal DatabaseDispatcher(TransactionSystem system) {
			this.system = system;
			thread = new Thread(new ThreadStart(run));
			thread.IsBackground = true;
			thread.Name = "Database Dispatcher";
			finished = false;
			thread.Start();
		}

		/// <summary>
		/// Creates an event object that is passed into <see cref="PostEvent"/> method 
		/// to run the given <see cref="Delegate"/> method after the time has passed.
		/// </summary>
		/// <param name="runnable"></param>
		/// <returns></returns>
		/// <remarks>
		/// The event created here can be safely posted on the event queue as many
		/// times as you like.  It's useful to create an event as a persistant object
		/// to service some event.  Just post it on the dispatcher when you want
		/// it run.
		/// </remarks>
		public object CreateEvent(IDatabaseEvent runnable) {
			return new DatabaseEvent(runnable);
		}

		/// <summary>
		/// Adds a new event to be dispatched on the queue after the given
		/// <paramref name="time_to_wait"/> milliseconds has passed.
		/// </summary>
		/// <param name="time_to_wait"></param>
		/// <param name="e"></param>
		public void PostEvent(int time_to_wait, Object e) {
			lock (this) {
				DatabaseEvent evt = (DatabaseEvent)e;
				// Remove this event from the queue,
				event_queue.Remove(e);
				// Set the correct time for the event.
				evt.time_to_run_event = DateTime.Now.AddMilliseconds(time_to_wait);
				// Add to the queue in correct order
				int index = event_queue.BinarySearch(e);
				if (index < 0) {
					index = -(index + 1);
				}
				event_queue.Insert(index, e);

				Monitor.PulseAll(this);
			}
		}

		/// <summary>
		/// Ends this dispatcher thread.
		/// </summary>
		public void Finish() {
			lock (this) {
				finished = true;
				Monitor.PulseAll(this);
			}
		}


		private void run() {
			while (true) {
				try {

					DatabaseEvent evt = null;
					lock (this) {
						while (evt == null) {
							// Return if finished
							if (finished) {
								return;
							}

							if (event_queue.Count > 0) {
								// Get the top entry, do we execute it yet?
								evt = (DatabaseEvent)event_queue[0];
								TimeSpan diff = evt.time_to_run_event - DateTime.Now;
								// If we got to wait for the event then do so now...
								if (diff.TotalMilliseconds >= 0) {
									evt = null;
									Monitor.Wait(this, diff);
								}
							} else {
								// Event queue empty so wait for someone to WriteByte an event on it.
								Monitor.Wait(this);
							}
						}
						// Remove the top event from the list,
						event_queue.RemoveAt(0);
					}

					// 'evt' is our event to run,
					evt.runnable.Execute();

				} catch (Exception e) {
					system.Debug.Write(DebugLevel.Error, this, "SystemDispatchThread error");
					system.Debug.WriteException(e);
				}
			}
		}

		// ---------- Inner classes ----------

		class DatabaseEvent : IComparable {
			internal DateTime time_to_run_event;
			internal readonly IDatabaseEvent runnable;

			internal DatabaseEvent(IDatabaseEvent runnable) {
				this.runnable = runnable;
			}

			public int CompareTo(Object ob) {
				DatabaseEvent evt2 = (DatabaseEvent)ob;
				TimeSpan dif = time_to_run_event - evt2.time_to_run_event;
				if (dif.TotalMilliseconds > 0) {
					return 1;
				} else if (dif.TotalMilliseconds < 0) {
					return -1;
				}
				return 0;
			}
		}
	}
}