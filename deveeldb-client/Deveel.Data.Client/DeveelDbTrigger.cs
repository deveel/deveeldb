using System;
using System.Collections;
using System.Data;
using System.Text;

namespace Deveel.Data.Client {
	public class DeveelDbTrigger : IDisposable {
		public DeveelDbTrigger(string triggerName, string onObject) {
			this.triggerName = triggerName;
			objectName = onObject;

			listener = new EventHandler(TriggerListener);
		}

		public DeveelDbTrigger(DeveelDbConnection connection, string triggerName, string onObject)
			: this(triggerName, onObject) {
			Connection = connection;
		}

		private readonly string triggerName;
		private readonly string objectName;
		private DeveelDbConnection connection;
		private TriggerEventType eventType = TriggerEventType.Insert | TriggerEventType.Update | TriggerEventType.Delete;
		private bool dropOnEmpty = true;
		private bool exists;

		private readonly EventHandler listener;

		private event EventHandler TriggerFired;

		public DeveelDbConnection Connection {
			get { return connection; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				connection = value;

				if (connection.State == ConnectionState.Open) {
					CheckExisting();
				} else {
					connection.StateChange += new StateChangeEventHandler(ConnectionStateChange);
				}

				EventHandler oldListener = connection.Driver.GetTriggerListener(triggerName);
				if (oldListener != null)
					TriggerFired += oldListener;
			}
		}

		public string TriggerName {
			get { return triggerName; }
		}

		public string OnObject {
			get { return objectName; }
		}

		public TriggerEventType EventType {
			get { return eventType; }
			set { eventType = value; }
		}

		public bool DropOnEmpty {
			get { return dropOnEmpty; }
			set { dropOnEmpty = value; }
		}

		private void ConnectionStateChange(object sender, StateChangeEventArgs e) {
			if (e.CurrentState == ConnectionState.Open)
				CheckExisting();
		}

		private void TriggerListener(object sender, EventArgs e) {
			if (TriggerFired != null)
				TriggerFired(this, e);
		}

		private void CheckExisting() {
			ParameterStyle paramStyle = connection.Settings.ParameterStyle;
			string commandText = "   SELECT " +
			                     "      IF(Triggers.schema IS NOT NULL, CONCAT(Triggers.schema,'.', Triggers.name), Triggers.name) AS NAME," +
			                     "      Triggers.on_object as ON_OBJECT " +
			                     "   FROM SYSTEM.sUSRDataTrigger AS Triggers " +
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

			using(DeveelDbDataReader reader = command.ExecuteReader()) {
				if (reader.Read()) {
					string onObject = reader.GetString(1);
					if (String.Compare(onObject, objectName, true) != 0)
						throw new ArgumentException("The trigger already exists and does not references '" + objectName + "'.");

					exists = true;
				}
			}
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

		internal virtual DeveelDbCommand GetDropStatement() {
			StringBuilder sb = new StringBuilder();
			sb.Append("DROP CALLBACK TRIGGER ");
			sb.Append(triggerName);

			return connection.CreateCommand(sb.ToString());
		}

		public void Subscribe(EventHandler e) {
			if (connection == null)
				throw new InvalidOperationException();

			if (!exists) {
				DeveelDbCommand command = GetCreateStatement();
				command.ExecuteNonQuery();
				exists = true;
			}

			if (TriggerFired == null)
				connection.Driver.AddTriggerListener(triggerName, listener);

			if (e != null)
				TriggerFired += e;
		}

		public void Unsubscribe(EventHandler e) {
			if (connection == null)
				throw new InvalidOperationException();

			if (!exists)
				throw new DataException("The trigger '" + triggerName + "' does not exist.");

			TriggerFired -= e;

			if (TriggerFired == null && DropOnEmpty) {
				DeveelDbCommand command = GetDropStatement();
				command.ExecuteNonQuery();
				connection.Driver.RemoveTriggerListener(triggerName, listener);
				exists = false;
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			try {
				if (TriggerFired != null)
					Unsubscribe(TriggerFired);
			} catch(Exception) {
				//TODO: log the error somewhere
			}
		}

		#endregion
	}
}