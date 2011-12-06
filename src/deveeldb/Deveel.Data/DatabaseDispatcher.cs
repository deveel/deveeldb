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
		private readonly List<object> eventQueue = new List<object>();

		private readonly TransactionSystem system;

		private bool finished;

		/**
		 * NOTE: Constructing this object will start the thread.
		 */
		internal DatabaseDispatcher(TransactionSystem system) {
			this.system = system;
			thread = new Thread(new ThreadStart(Run));
			thread.IsBackground = true;
			thread.Name = "Database Dispatcher";
			finished = false;
			thread.Start();
		}

		/// <summary>
		/// Creates an event object that is passed into <see cref="PostEvent"/> method 
		/// to run the given <see cref="Delegate"/> method after the time has passed.
		/// </summary>
		/// <param name="callback"></param>
		/// <returns></returns>
		/// <remarks>
		/// The event created here can be safely posted on the event queue as many
		/// times as you like.  It's useful to create an event as a persistant object
		/// to service some event.  Just post it on the dispatcher when you want
		/// it run.
		/// </remarks>
		public object CreateEvent(EventHandler callback) {
			return new DatabaseEvent(callback);
		}

		/// <summary>
		/// Adds a new event to be dispatched on the queue after the given
		/// <paramref name="timeToWait"/> milliseconds has passed.
		/// </summary>
		/// <param name="timeToWait"></param>
		/// <param name="e"></param>
		public void PostEvent(int timeToWait, object e) {
			lock (this) {
				DatabaseEvent evt = (DatabaseEvent)e;
				// Remove this event from the queue,
				eventQueue.Remove(e);
				// Set the correct time for the event.
				evt.TimeToRunEvent = DateTime.Now.AddMilliseconds(timeToWait);
				// Add to the queue in correct order
				int index = eventQueue.BinarySearch(e);
				if (index < 0) {
					index = -(index + 1);
				}
				eventQueue.Insert(index, e);

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


		private void Run() {
			while (true) {
				try {
					DatabaseEvent evt = null;
					lock (this) {
						while (evt == null) {
							// Return if finished
							if (finished)
								return;

							if (eventQueue.Count > 0) {
								// Get the top entry, do we execute it yet?
								evt = (DatabaseEvent)eventQueue[0];
								TimeSpan diff = evt.TimeToRunEvent - DateTime.Now;
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
						eventQueue.RemoveAt(0);
					}

					// 'evt' is our event to run,
					evt.Callback(this, EventArgs.Empty);

				} catch (Exception e) {
					system.Debug.Write(DebugLevel.Error, this, "SystemDispatchThread error");
					system.Debug.WriteException(e);
				}
			}
		}

		// ---------- Inner classes ----------

		class DatabaseEvent : IComparable {
			public DateTime TimeToRunEvent;
			public readonly EventHandler Callback;

			internal DatabaseEvent(EventHandler callback) {
				Callback = callback;
			}

			public int CompareTo(Object ob) {
				DatabaseEvent evt2 = (DatabaseEvent)ob;
				TimeSpan dif = TimeToRunEvent - evt2.TimeToRunEvent;
				if (dif.TotalMilliseconds > 0)
					return 1;
				if (dif.TotalMilliseconds < 0)
					return -1;
				return 0;
			}
		}
	}
}