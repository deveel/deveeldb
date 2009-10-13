//  
//  DeveelDbTrigger.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Text;

namespace Deveel.Data.Client {
	public class DeveelDbTrigger : IDisposable {
		public DeveelDbTrigger(DeveelDbConnection connection, string triggerName, string objectName) {
			this.connection = connection;
			this.triggerName = triggerName;
			this.objectName = objectName;

			exists = CallInit();
			listener = new TriggerListener(this);
		}

		private string triggerName;
		private readonly string objectName;
		private readonly DeveelDbConnection connection;
		private TriggerEventTypes eventType = TriggerEventTypes.All;
		private bool exists;
		private readonly ITriggerListener listener;

		internal DeveelDbConnection Connection {
			get { return connection; }
		}

		public string ObjectName {
			get { return objectName; }
		}

		public string Name {
			get { return triggerName; }
		}

		public TriggerEventTypes EventTypes {
			get { return eventType; }
			set {
				CheckExisting();
				eventType = value;
			}
		}

		private event EventHandler TriggerFired;

		// make sure the compiler doesn't throw a warning by calling
		// a virtual method in constructor...
		private bool CallInit() {
			return Init();
		}

		internal virtual bool Init() {
			ParameterStyle paramStyle = connection.Settings.ParameterStyle;
			string commandText = "   SELECT " +
			                     "      IF(Triggers.schema IS NOT NULL, CONCAT(Triggers.schema,\".\", Triggers.name), Triggers.name) AS NAME," + 
                                 "      Triggers.on_object as ON_OBJECT " +
								 "   FROM SYSTEM.sUSRDataTrigger AS Triggers " +
								 "   WHERE Triggers.name = ";
			if (paramStyle == ParameterStyle.Marker)
				commandText += "?";
			else
				commandText += "@TriggerName";
			commandText += ";";

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

			return false;
		}

		internal void CheckExisting() {
			if (exists)
				throw new InvalidOperationException("The trigger already exists.");
		}

		internal static string FormatEventType(TriggerEventTypes types) {
			ArrayList list = new ArrayList();
			if ((types & TriggerEventTypes.Insert) != 0)
				list.Add(TriggerEventTypes.Insert);
			if ((types & TriggerEventTypes.Update) != 0)
				list.Add(TriggerEventTypes.Update);
			if ((types & TriggerEventTypes.Delete) != 0)
				list.Add(TriggerEventTypes.Delete);

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < list.Count; i++) {
				sb.Append(((TriggerEventTypes) list[i]).ToString().ToUpper());
				if (i < list.Count - 1)
					sb.Append(" OR ");
			}
			return sb.ToString();
		}

		internal virtual DeveelDbCommand GetCreateStatement() {
			ParameterStyle paramStyle = connection.Settings.ParameterStyle;

			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE CALLBACK TRIGGER ");
			if (paramStyle == ParameterStyle.Marker) {
				sb.Append("?");
			} else {
				sb.Append("@TriggerName");
			}
			sb.Append(" ");
			sb.Append(FormatEventType(eventType));
			sb.Append(" ON ");
			if (paramStyle == ParameterStyle.Marker) {
				sb.Append("?");
			} else {
				sb.Append("@TableName");
			}

			sb.Append(";");

			DeveelDbCommand command = connection.CreateCommand(sb.ToString());
			if (paramStyle == ParameterStyle.Marker) {
				command.Parameters.Add(triggerName);
				command.Parameters.Add(objectName);
			} else {
				command.Parameters.Add("@TriggerName", triggerName);
				command.Parameters.Add("@TableName", objectName);
			}

			command.Prepare();

			return command;
		}

		internal virtual DeveelDbCommand GetDropStatement() {
			ParameterStyle paramStyle = connection.Settings.ParameterStyle;

			StringBuilder sb = new StringBuilder();
			sb.Append("DROP CALLBACK TRIGGER ");
			if (paramStyle == ParameterStyle.Marker)
				sb.Append("?");
			else
				sb.Append("@TriggerName");
			sb.Append(";");

			DeveelDbCommand command = connection.CreateCommand(sb.ToString());

			if (paramStyle == ParameterStyle.Marker)
				command.Parameters.Add(triggerName);
			else
				command.Parameters.Add("@TriggerName", triggerName);

			command.Prepare();

			return command;
		}

		public void Dispose() {
			try {
				if (TriggerFired != null)
					Unsubscribe(TriggerFired);
			} catch {
				
			}
		}

		public void Subscribe(EventHandler e) {
			if (!exists) {
				DeveelDbCommand command = GetCreateStatement();
				command.ExecuteNonQuery();
				connection.AddTriggerListener(triggerName, listener);
				exists = true;
			}

			if (e != null)
				TriggerFired += e;
		}

		public void Unsubscribe(EventHandler e) {
			if (!exists)
				throw new InvalidOperationException();

			TriggerFired -= e;

			if (TriggerFired == null) {
				DeveelDbCommand command = GetDropStatement();
				command.ExecuteNonQuery();
				exists = false;
			}
		}

		private class TriggerListener : ITriggerListener {
			public TriggerListener(DeveelDbTrigger trigger) {
				this.trigger = trigger;
			}

			private readonly DeveelDbTrigger trigger;

			public void OnTriggerFired(string trigger_name) {
				if (trigger_name != trigger.Name)
					return;

				if (trigger.TriggerFired != null)
					trigger.TriggerFired(trigger, EventArgs.Empty);
			}
		}
	}
}