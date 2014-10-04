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
using System.Collections.Generic;
using System.Text;

using Deveel.Data.Protocol;
using Deveel.Data.Routines;

namespace Deveel.Data.Client {
	public sealed class DeveelDbTrigger : IDisposable {
		private ITriggerChannel channel;

		public DeveelDbTrigger(string triggerName, TriggerEventType eventType) 
			: this((DeveelDbConnection) null, triggerName, eventType) {
		}

		public DeveelDbTrigger(DeveelDbConnection connection, string triggerName, TriggerEventType eventType) {
			if (String.IsNullOrEmpty(triggerName))
				throw new ArgumentNullException("triggerName");

			Connection = connection;
			TriggerName = triggerName;
			EventType = eventType;
		}

		public DeveelDbTrigger(string connectionString, string triggerName, TriggerEventType eventType)
			: this(new DeveelDbConnection(connectionString), triggerName, eventType) {
			OwnsConnection = true;
		}

		~DeveelDbTrigger() {
			Dispose(false);
		}

		public string TriggerName { get; private set; }

		public string ObjectName { get; set; }

		public TriggerEventType EventType { get; set; }

		public DeveelDbConnection Connection { get; set; }

		private bool OwnsConnection { get; set; }

		private DeveelDbCommand GetCreateStatement() {
			var sb = new StringBuilder();
			sb.Append("CREATE CALLBACK TRIGGER ");
			sb.Append(TriggerName);
			sb.Append(" ");
			sb.Append(FormatEventType(EventType));
			sb.Append(" ON ");
			sb.Append(ObjectName);

			return Connection.CreateCommand(sb.ToString());
		}

		private DeveelDbCommand GetDropStatement() {
			var sb = new StringBuilder();
			sb.Append("DROP CALLBACK TRIGGER ");
			sb.Append(TriggerName);

			return Connection.CreateCommand(sb.ToString());
		}

		private static string FormatEventType(TriggerEventType types) {
			var list = new List<TriggerEventType>();
			bool before = false, after = false;
			if ((types & TriggerEventType.Before) != 0)
				before = true;
			if ((types & TriggerEventType.After) != 0)
				after = true;

			if (!before && !after)
				throw new InvalidOperationException("The event type for the trigger must be either BEFORE or AFTER.");

			if ((types & TriggerEventType.Insert) != 0)
				list.Add(TriggerEventType.Insert);
			if ((types & TriggerEventType.Update) != 0)
				list.Add(TriggerEventType.Update);
			if ((types & TriggerEventType.Delete) != 0)
				list.Add(TriggerEventType.Delete);

			var sb = new StringBuilder();

			if (before) {
				sb.Append("BEFORE");
			} else {
				sb.Append("AFTER");
			}

			sb.Append(" ");

			for (int i = 0; i < list.Count; i++) {
				sb.Append(list[i].ToString().ToUpper());
				if (i < list.Count - 1)
					sb.Append(" OR ");
			}

			return sb.ToString();
		}

		public void Create() {
			if (EventType == 0)
				throw new InvalidOperationException("The trigger is not set to any event.");

			if (channel != null)
				throw new InvalidOperationException("There is a channel open for the trigger: it already exists");

			if (Connection == null)
				throw new InvalidOperationException("The trigger is outside context.");

			if (String.IsNullOrEmpty(ObjectName))
				throw new InvalidOperationException("The object name was not set.");

			var command = GetCreateStatement();
			var result = command.ExecuteNonQuery();
			if (result != 0)
				throw new InvalidOperationException("Was not able to create the trigger on the server.");
		}

		public void Drop() {
			try {
				var command = GetDropStatement();
				var result = command.ExecuteNonQuery();
				if (result != 1)
					throw new InvalidOperationException("Was not able to drop the trigger on the server.");
			} catch (Exception) {

				throw;
			} finally {
				Unsubscribe();
			}
		}

		public void Subscribe(Action<TriggerInvoke> callback) {
			if (channel != null)
				throw new InvalidOperationException("The trigger was alredy subscribed.");

			if (Connection == null)
				throw new InvalidOperationException("The trigger is outside context.");

			channel = Connection.OpenTriggerChannel(TriggerName, ObjectName, EventType);
			channel.OnTriggeInvoked(n => OnTriggered(n, callback));
		}

		private void OnTriggered(TriggerEventNotification notification, Action<TriggerInvoke> callback) {
			if (callback != null)
				callback(new TriggerInvoke(notification.TriggerName, notification.ObjectName, notification.EventType, notification.InvokeCount));
		}

		public void Unsubscribe() {
			if (channel == null)
				return;

			try {
				channel.Dispose();
			} catch (Exception) {

				throw;
			} finally {
				channel = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					Unsubscribe();

					if (OwnsConnection && Connection != null)
						Connection.Dispose();
				} catch {
					// Ignore any exception here
				} finally {
					Connection = null;
				}
			}
		}

		public static DeveelDbTrigger Subscribe(string connectionString, string triggerName, TriggerEventType eventType, Action<TriggerInvoke> callback) {
			var trigger = new DeveelDbTrigger(connectionString, triggerName, eventType);
			trigger.Subscribe(callback);
			return trigger;
		}

		public static DeveelDbTrigger Create(string connectionString, string triggerName, string objectName,
			TriggerEventType eventType) {
			return Create(connectionString, triggerName, objectName, eventType, null);
		}

		public static DeveelDbTrigger Create(string connectionString, string triggerName, string objectName,
			TriggerEventType eventType, Action<TriggerInvoke> callback) {
			var trigger = new DeveelDbTrigger(connectionString, triggerName, eventType);
			trigger.ObjectName = objectName;
			trigger.Create();

			if (callback != null)
				trigger.Subscribe(callback);

			return trigger;
		}
	}
}