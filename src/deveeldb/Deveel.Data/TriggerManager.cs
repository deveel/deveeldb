// 
//  Copyright 2010-2011  Deveel
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
using System.Collections.Generic;

using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// An object that manages high level trigger events within a 
	/// <see cref="Database"/> context.
	/// </summary>
	/// <remarks>
	/// This manager is designed to manage the map between session and triggers
	/// being listened for. It is the responsibility of the language parsing
	/// layer to notify this manager of trigger events.
	/// <para>
	/// It is intended that this object manages events from the highest layer, 
	/// so it is possible that trigger events may not get to be notified if queries 
	/// are not evaluated properly.
	/// </para>
	/// <para>
	/// This object is only intended as a helper for implementing a trigger event 
	/// dispatcher by a higher level package (eg. <see cref="Deveel.Data.Sql"/>).
	/// </para>
	/// <para>
	/// <b>Concurrency</b> This class is thread safe. It may safely be accessed 
	/// by multiple threads. Any events that are fired are put on the 
	/// <see cref="DatabaseDispatcher"/> thread.
	/// </para>
	/// </remarks>
	sealed class TriggerManager {
		/// <summary>
		/// The parent TransactionSystem object.
		/// </summary>
		private readonly TransactionSystem system;
		/// <summary>
		/// Maps from the user session (User) to the list of TriggerAction 
		/// objects for this user.
		/// </summary>
		private readonly HashMapList listenerMap;

		/// <summary>
		/// Maps from the trigger source string to the list of TriggerAction
		/// objects that are listening for events from this source.
		/// </summary>
		private readonly HashMapList tableMap;

		internal TriggerManager(TransactionSystem system) {
			this.system = system;
			listenerMap = new HashMapList();
			tableMap = new HashMapList();
		}

		/// <summary>
		/// Flushes the list of <see cref="TriggerEventArgs"/> objects and 
		/// dispatches them to the users that are listening.
		/// </summary>
		/// <param name="eventList"></param>
		/// <remarks>
		/// This is called after the given connection has successfully 
		/// committed and closed.
		/// </remarks>
		internal void FlushTriggerEvents(IEnumerable<TriggerEventArgs> eventList) {
			foreach (TriggerEventArgs args in eventList) {
				FireTrigger(args);
			}
		}

		/// <summary>
		/// Adds a handler for an event with the given 'id' for the user session.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="triggerName"></param>
		/// <param name="eventType"></param>
		/// <param name="triggerSource"></param>
		/// <param name="handler"></param>
		/// <example>
		/// In the following example the handler is notified of all update 
		/// events on the 'Part' table:
		/// <code>
		/// AddTrigger(user, "my_trigger", TriggerEventType.Update, "Part", my_handler);
		/// </code>
		/// </example>
		internal void AddTrigger(DatabaseConnection database, string triggerName, TriggerEventType eventType, string triggerSource, TriggerEventHandler handler) {
			lock (this) {
				// Has this trigger name already been defined for this user?
				IList list = listenerMap[database];
				foreach (TriggerAction act in list) {
					if (act.Name.Equals(triggerName))
						throw new ApplicationException("Duplicate trigger name '" + triggerName + "'");
				}

				TriggerAction action = new TriggerAction(database, triggerName, eventType, triggerSource, handler);

				listenerMap.Add(database, action);
				tableMap.Add(triggerSource, action);
			}
		}

		/// <summary>
		/// Removes a trigger for the given user session.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="triggerName"></param>
		internal void RemoveTrigger(DatabaseConnection database, string triggerName) {
			lock (this) {
				IList list = listenerMap[database];
				foreach (TriggerAction action in list) {
					if (action.Name.Equals(triggerName)) {
						listenerMap.Remove(database, action);
						tableMap.Remove(action.TriggerSource, action);
						return;
					}
				}
				throw new ApplicationException("Trigger name '" + triggerName + "' not found.");
			}
		}

		/// <summary>
		/// Clears all the user triggers that have been defined.
		/// </summary>
		/// <param name="database"></param>
		internal void ClearAllDatabaseConnectionTriggers(DatabaseConnection database) {
			lock (this) {
				IList list = listenerMap.Clear(database);
				foreach (TriggerAction action in list) {
					tableMap.Remove(action.TriggerSource, action);
				}
			}
		}

		/// <summary>
		/// Notifies all the handlers on a triggerSource (ie. a table) that a
		/// specific type of event has happened, as denoted by the type.
		/// </summary>
		/// <param name="args"></param>
		private void FireTrigger(TriggerEventArgs args) {
			ArrayList trigList;
			// Get all the triggers for this trigger source,
			lock (this) {
				IList list = tableMap[args.Source];
				if (list.Count == 0)
					return;

				trigList = new ArrayList(list);
			}

			// Post an event that fires the triggers for each handler.

			// Post the event to go off approx 3ms from now.
			system.PostEvent(3, system.CreateEvent(delegate {
			                                       	foreach (TriggerAction action in trigList) {
			                                       		if ((args.Type & action.EventType) != 0) {
			                                       			action.Handler(action.Connection, args);
			                                       		}
			                                       	}
			                                       }));
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// Encapsulates the informations of a trigger event handler for a specific 
		/// event for a user.
		/// </summary>
		private sealed class TriggerAction {
			private readonly DatabaseConnection connection;
			private readonly string triggerName;   // The name of the trigger.
			private readonly TriggerEventHandler handler;       // The trigger handler.
			private readonly string triggerSource; // The source of the trigger.
			private readonly TriggerEventType eventType;  // Event we are to listen for.

			internal TriggerAction(DatabaseConnection connection, string triggerName, TriggerEventType eventType, string triggerSource, TriggerEventHandler handler) {
				this.connection = connection;
				this.triggerName = triggerName;
				this.eventType = eventType;
				this.handler = handler;
				this.triggerSource = triggerSource;
			}

			/// <summary>
			/// Returns the name of the trigger.
			/// </summary>
			public string Name {
				get { return triggerName; }
			}

			public TriggerEventHandler Handler {
				get { return handler; }
			}

			public string TriggerSource {
				get { return triggerSource; }
			}

			public TriggerEventType EventType {
				get { return eventType; }
			}

			public DatabaseConnection Connection {
				get { return connection; }
			}
		}
	}
}