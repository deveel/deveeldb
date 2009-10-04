//  
//  DatabaseDispatcher.cs
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
		public object CreateEvent(EventHandler runnable) {
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
					evt.runnable(system, EventArgs.Empty);

				} catch (Exception e) {
					Debug.Write(DebugLevel.Error, this, "SystemDispatchThread error");
					Debug.WriteException(e);
				}
			}
		}

		// ---------- Inner classes ----------

		class DatabaseEvent : IComparable {
			internal DateTime time_to_run_event;
			internal readonly EventHandler runnable;

			internal DatabaseEvent(EventHandler runnable) {
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