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
using System.Data;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Sql;

namespace Deveel.Data.Client {
	/// <summary>
	/// Represents a trigger in a connection to a database.
	/// </summary>
	/// <remarks>
	/// Triggers are events fired at defined moments during a data modification 
	/// command (<c>INSERT</c>, <c>DELETE</c> or <c>UPDATE</c>).
	/// <para>
	/// Instatiating a new <see cref="DeveelDbTrigger"/> does not make it active:
	/// to start listening to trigger events it is needed to issue a call
	/// to <see cref="Subscribe"/> at least once.
	/// </para>
	/// </remarks>
	/// <example>
	/// In the following example the code creates a trigger, named <b>CustomerCreated</b>, 
	/// which listens for the <c>INSERT</c> event on the table <b>APP.Customers</b>.
	/// <code lang="c#">
	/// namespace Foo.Bar {
	///		public sealed class Program {
	///			public static int Main(string[] args) {
	///				DeveelDbConnection conn = ...
	///				DeveelDbTrigger trigger = new DeveelDbTrigger(conn, "CustomerCreated", "APP.Customers");
	///				trigger.EventTypes = TriggerEventTypes.Insert;
	///             trigger.Subscribe(new EventHandler(OnCustomerCreated));
	///			}
	/// 
	///			private void OnCustomerCreated(object sender, EventArgs e) {
	///				DeveelDbTrigger trigger = (DeveelDbTrigger)sender;
	///				...
	///			}
	///		}
	/// }
	/// </code>
	/// </example>
	public class DeveelDbTrigger : IDisposable {
		/// <summary>
		/// Constructs a new <see cref="DeveelDbTrigger"/> with the given
		/// name which listens to event on the given database object.
		/// </summary>
		/// <param name="connection">The connection on which the trigger exists.</param>
		/// <param name="triggerName">The name of the trigger.</param>
		/// <param name="objectName">The name of the object (table, view, etc.) for which
		/// to listen to events.</param>
		/// <remarks>
		/// If the connection is opened, this method will query the database
		/// for a trigger with the same name: if one was already found, this
		/// will prevent the creation of a new <c>trigger</c>.
		/// <para>
		/// Triggers are events which live only for the time of a connection: at
		/// the disposal of the connection, these will be discarded and no more
		/// events will be fired at the callbacks.
		/// </para>
		/// <para>
		/// This object will start firing events related to the database object given
		/// only after the method <see cref="Subscribe"/> will be called.
		/// To stop listening to events on the database object it will be needed a call
		/// to <see cref="Unsubscribe"/> or <see cref="Dispose"/> (to stop listening to
		/// every event).
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If either one of the given <paramref name="connection"/>, <paramref name="triggerName"/> 
		/// or <paramref name="objectName"/> are <b>null</b>.
		/// </exception>
		public DeveelDbTrigger(DeveelDbConnection connection, string triggerName, string objectName) {
			if (connection == null)
				throw new ArgumentNullException("connection");
			if (string.IsNullOrEmpty(triggerName))
				throw new ArgumentNullException("triggerName");
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			this.connection = connection;
			this.triggerName = triggerName;
			this.objectName = objectName;

			exists = CallInit();
			handler = Fired;
		}

		~DeveelDbTrigger() {
			Dispose(false);
		}

		/// <summary>
		/// The name of the trigger.
		/// </summary>
		private string triggerName;

		/// <summary>
		/// The name of the database object for which to fire the trigger.
		/// </summary>
		private readonly string objectName;

		/// <summary>
		/// The containing connection.
		/// </summary>
		private readonly DeveelDbConnection connection;

		/// <summary>
		/// The types of commands to listen for.
		/// </summary>
		private TriggerEventType eventType = TriggerEventType.Insert | TriggerEventType.Update | TriggerEventType.Delete;

		/// <summary>
		/// Indicates whether the trigger already exists in the database.
		/// </summary>
		private bool exists;

		/// <summary>
		/// A callback object which listens for triggers on the database.
		/// </summary>
		private readonly TriggerEventHandler handler;

		/// <summary>
		/// A flag used to indicate whether this trigger was initialized.
		/// </summary>
		private bool initd;

		/// <summary>
		/// A flag indicating whether this trigger must be dropped when
		/// all the events were unsubscribed.
		/// </summary>
		private bool dropOnEmpty = true;

		/// <summary>
		/// Gets the <see cref="DeveelDbConnection"/> which contains the
		/// trigger.
		/// </summary>
		public DeveelDbConnection Connection {
			get { return connection; }
		}

		/// <summary>
		/// Gets the name of the database object for which the trigger is fired.
		/// </summary>
		public string ObjectName {
			get { return objectName; }
		}

		/// <summary>
		/// Gets the name of the trigger.
		/// </summary>
		public string Name {
			get { return triggerName; }
		}

		/// <summary>
		/// Gets or sets the types of data modification commands for which
		/// to register the trigger.
		/// </summary>
		/// <remarks>
		/// By default a callback trigger listens to all event types.
		/// </remarks>
		public TriggerEventType EventType {
			get { return eventType; }
			set { eventType = value; }
		}

		/// <summary>
		/// Indicates whether is this trigger will be dropped from the
		/// database when all the registered events are unsubscribed.
		/// </summary>
		/// <seealso cref="Unsubscribe"/>
		public bool DropOnEmpty {
			get { return dropOnEmpty; }
			set { dropOnEmpty = value; }
		}

		/// <summary>
		/// A delegate method used to store the registered events fired
		/// when the trigger happens.
		/// </summary>
		private event EventHandler TriggerFired;

		// make sure the compiler doesn't throw a warning by calling
		// a virtual method in constructor...
		private bool CallInit() {
			if (initd)
				return exists;

			return Init();
		}

		protected virtual  void Dispose(bool disposing) {
			if (disposing) {
				try {
					if (TriggerFired != null)
						Unsubscribe(TriggerFired);
				} catch {
					// we ignore this error on destruction...
				}
			}
		}

		internal virtual bool Init() {
			if (connection.State != ConnectionState.Open)
				return false;

			ParameterStyle paramStyle = connection.Settings.ParameterStyle;
			string commandText = "   SELECT " +
			                     "      IIF(Triggers.schema IS NOT NULL, CONCAT(Triggers.schema,'.', Triggers.name), Triggers.name) AS NAME," + 
                                 "      Triggers.on_object as ON_OBJECT " +
								 "   FROM SYSTEM.data_trigger AS Triggers " +
								 "   WHERE Triggers.name = ";
			if (paramStyle == ParameterStyle.Marker)
				commandText += "?";
			else
				commandText += "@TriggerName";

			DeveelDbCommand command = connection.CreateCommand(commandText);
			if (paramStyle == ParameterStyle.Marker)
				command.Parameters.Add(triggerName);
			else
				command.Parameters.Add("@TriggerName", triggerName);
			command.Prepare();

			DeveelDbDataReader reader = command.ExecuteReader();
			try {
				if (reader.Read()) {
					// if the trigger already exists, adjust the name with the schema...
					triggerName = reader.GetString(0);
					string onObject = reader.GetString(1);
					if (String.Compare(onObject, objectName, true) != 0)
						throw new ArgumentException("The trigger already exists and does not references '" + objectName + "'.");
					return true;
				}
			} finally {
				reader.Close();
			}

			initd = true;
			return false;
		}

		internal static string FormatEventType(TriggerEventType types) {
			ArrayList list = new ArrayList();
			bool before = false, after = false;
			if ((types & TriggerEventType.Before) != 0)
				before = true;
			if ((types & TriggerEventType.After) != 0)
				after = true;

			if ((types & TriggerEventType.Insert) != 0)
				list.Add(TriggerEventType.Insert);
			if ((types & TriggerEventType.Update) != 0)
				list.Add(TriggerEventType.Update);
			if ((types & TriggerEventType.Delete) != 0)
				list.Add(TriggerEventType.Delete);

			StringBuilder sb = new StringBuilder();

			if (before | after) {
				if (before) {
					for (int i = 0; i < list.Count; i++) {
						sb.Append("BEFORE");
						sb.Append(' ');
						sb.Append(((TriggerEventType)list[i]).ToString().ToUpper());
						if (i < list.Count - 1)
							sb.Append(" OR ");
					}
				}

				if (before && after)
					sb.Append(" OR ");

				if (after) {
					for (int i = 0; i < list.Count; i++) {
						sb.Append("AFTER");
						sb.Append(' ');
						sb.Append(((TriggerEventType)list[i]).ToString().ToUpper());
						if (i < list.Count - 1)
							sb.Append(" OR ");
					}
				}
			} else {
				for (int i = 0; i < list.Count; i++) {
					sb.Append(((TriggerEventType)list[i]).ToString().ToUpper());
					if (i < list.Count - 1)
						sb.Append(" OR ");
				}
			}

			return sb.ToString();
		}

		/// <summary>
		/// Gets a statement used to register the trigger.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="DeveelDbCommand"/> which encapsulates the statement
		/// used to create a new trigger on the database.
		/// </returns>
		internal virtual DeveelDbCommand GetCreateStatement() {
			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE CALLBACK TRIGGER ");
			sb.Append(triggerName);
			sb.Append(" ");
			sb.Append(FormatEventType(eventType));
			sb.Append(" ON ");
			sb.Append(objectName);

			return connection.CreateCommand(sb.ToString());
		}

		/// <summary>
		/// Gets a statement used to destroy the trigger.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="DeveelDbCommand"/> which encapsulates the statement
		/// used to drop an existing trigger on the database.
		/// </returns>
		internal virtual DeveelDbCommand GetDropStatement() {
			StringBuilder sb = new StringBuilder();
			sb.Append("DROP CALLBACK TRIGGER ");
			sb.Append(triggerName);

			return connection.CreateCommand(sb.ToString());
		}

		/// <inheritdoc/>
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Subscribes to the events fired by the trigger on the database.
		/// </summary>
		/// <param name="e">The <see cref="EventHandler"/> which is registered to listen
		/// to events.</param>
		/// <remarks>
		/// This method works in the following way:
		/// <list type="bullet">
		///   <item>ensures the <see cref="Connection"/> is opened</item>
		///   <item>if not already checked at construction, it verifies if
		///   a trigger with the same name already exists</item>
		///   <item>if not already found, it will issue a SQL statement
		///   to create a new trigger for the underlying <see cref="Connection"/></item>
		///   <item>registers the given event handler to listen for events
		///   related to the trigger.</item>
		/// </list>
		/// </remarks>
		public void Subscribe(EventHandler e) {
			CallInit();

			if (!exists) {
				DeveelDbCommand command = GetCreateStatement();
				command.ExecuteNonQuery();
				exists = true;
			}

			if (e != null)
				TriggerFired += e;
		}

		/// <summary>
		/// Unsubscribes the given event handler from the listening of events
		/// fired by the trigger on the database.
		/// </summary>
		/// <param name="e">The <see cref="EventHandler"/> delegate method to
		/// unregister.</param>
		/// <remarks>
		/// If all the events are unregistered from the trigger, this method
		/// will issue a call to the database to drop it definitively, if
		/// <see cref="DropOnEmpty"/> was set to <b>true</b>.
		/// </remarks>
		/// <seealso cref="DropOnEmpty"/>
		public void Unsubscribe(EventHandler e) {
			CallInit();

			if (!exists)
				throw new DataException("The trigger '" + triggerName + "' does not exist.");

			TriggerFired -= e;

			if (TriggerFired == null && DropOnEmpty) {
				DeveelDbCommand command = GetDropStatement();
				command.ExecuteNonQuery();
				connection.RemoveTriggerListener(triggerName, handler);
				exists = false;
			}
		}

		private void Fired(object sender, TriggerEventArgs args) {
			if (args.TriggerName != Name)
				return;

			if (TriggerFired != null)
				TriggerFired(this, args);
		}
	}
}