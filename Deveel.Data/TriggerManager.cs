// 
//  TriggerManager.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

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
		private TransactionSystem system;
		/// <summary>
		/// Maps from the user session (User) to the list of TriggerAction 
		/// objects for this user.
		/// </summary>
		private HashMapList listener_map;

		/// <summary>
		/// Maps from the trigger source string to the list of TriggerAction
		/// objects that are listening for events from this source.
		/// </summary>
		private HashMapList table_map;

		internal TriggerManager(TransactionSystem system) {
			this.system = system;
			listener_map = new HashMapList();
			table_map = new HashMapList();
		}

		/// <summary>
		/// Flushes the list of <see cref="TriggerEvent"/> objects and 
		/// dispatches them to the users that are listening.
		/// </summary>
		/// <param name="event_list"></param>
		/// <remarks>
		/// This is called after the given connection has successfully 
		/// committed and closed.
		/// </remarks>
		internal void FlushTriggerEvents(ArrayList event_list) {
			for (int i = 0; i < event_list.Count; ++i) {
				TriggerEvent evt = (TriggerEvent)event_list[i];
				FireTrigger(evt);
			}
		}

		/// <summary>
		/// Adds a listener for an event with the given 'id' for the user session.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="trigger_name"></param>
		/// <param name="event_id"></param>
		/// <param name="trigger_source"></param>
		/// <param name="listener"></param>
		/// <example>
		/// In the following example the handler is notified of all update 
		/// events on the 'Part' table:
		/// <code>
		/// AddTriggerHandler(user, "my_trigger", TriggerEventType.Update, "Part", my_handler);
		/// </code>
		/// </example>
		internal void AddTriggerListener(DatabaseConnection database,
					   String trigger_name, TriggerEventType event_id, String trigger_source,
													   ITriggerListener listener) {
			lock (this) {
				// Has this trigger name already been defined for this user?
				IList list = listener_map[database];
				for (int i = 0; i < list.Count; ++i) {
					TriggerAction act = (TriggerAction)list[i];
					if (act.Name.Equals(trigger_name)) {
						throw new ApplicationException("Duplicate trigger name '" + trigger_name + "'");
					}
				}

				TriggerAction action = new TriggerAction(database, trigger_name, event_id,
														 trigger_source, listener);

				listener_map.Add(database, action);
				table_map.Add(trigger_source, action);
			}
		}

		/// <summary>
		/// Removes a trigger for the given user session.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="trigger_name"></param>
		internal void RemoveTriggerListener(DatabaseConnection database,
												String trigger_name) {
			lock (this) {
				IList list = listener_map[database];
				for (int i = 0; i < list.Count; ++i) {
					TriggerAction action = (TriggerAction)list[i];
					if (action.Name.Equals(trigger_name)) {
						listener_map.Remove(database, action);
						table_map.Remove(action.trigger_source, action);
						return;
					}
				}
				throw new ApplicationException("Trigger name '" + trigger_name + "' not found.");
			}
		}

		/// <summary>
		/// Clears all the user triggers that have been defined.
		/// </summary>
		/// <param name="database"></param>
		internal void ClearAllDatabaseConnectionTriggers(DatabaseConnection database) {
			lock (this) {
				IList list = listener_map.Clear(database);
				for (int i = 0; i < list.Count; ++i) {
					TriggerAction action = (TriggerAction)list[i];
					table_map.Remove(action.trigger_source, action);
				}
			}
		}

		/// <summary>
		/// Notifies all the handlers on a triggerSource (ie. a table) that a
		/// specific type of event has happened, as denoted by the type.
		/// </summary>
		/// <param name="e"></param>
		private void FireTrigger(TriggerEvent evt) {

			ArrayList trig_list;
			// Get all the triggers for this trigger source,
			//    Console.Out.WriteLine(evt.getSource());
			//    Console.Out.WriteLine(table_map);
			lock (this) {
				IList list = table_map[evt.Source];
				if (list.Count == 0) {
					return;
				}
				trig_list = new ArrayList(list);
			}

			// Post an event that fires the triggers for each listener.
			FireTriggersDelegate d = new FireTriggersDelegate(evt, trig_list);
			EventHandler runner = new EventHandler(d.fireTriggers);

			// Post the event to go off approx 3ms from now.
			system.PostEvent(3, system.CreateEvent(runner));

		}

		private class FireTriggersDelegate {
			public FireTriggersDelegate(TriggerEvent evt, ArrayList trig_list) {
				this.evt = evt;
				this.trig_list = trig_list;
			}

			private readonly ArrayList trig_list;
			private readonly TriggerEvent evt;

			public void fireTriggers(object sender, EventArgs e) {
				for (int i = 0; i < trig_list.Count; ++i) {
					TriggerAction action = (TriggerAction)trig_list[i];
					if (evt.Type == action.trigger_event) {
						action.listener.FireTrigger(action.database, action.trigger_name,
													evt);
					}
				}
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// Encapsulates the informations of a trigger event handler for a specific 
		/// event for a user.
		/// </summary>
		private sealed class TriggerAction {
			internal DatabaseConnection database;
			internal String trigger_name;   // The name of the trigger.
			internal ITriggerListener listener;       // The trigger listener.
			internal String trigger_source; // The source of the trigger.
			internal TriggerEventType trigger_event;  // Event we are to listen for.

			internal TriggerAction(DatabaseConnection database, String name, TriggerEventType type,
						  String trigger_source, ITriggerListener listener) {
				this.database = database;
				this.trigger_name = name;
				this.trigger_event = type;
				this.listener = listener;
				this.trigger_source = trigger_source;
			}

			/// <summary>
			/// Returns the name of the trigger.
			/// </summary>
			public string Name {
				get { return trigger_name; }
			}
		}
	}
}