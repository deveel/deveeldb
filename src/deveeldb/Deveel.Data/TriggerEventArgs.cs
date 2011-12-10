// 
//  Copyright 2011 Deveel
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

namespace Deveel.Data {
	public delegate void TriggerEventHandler(object sender, TriggerEventArgs args);

	public sealed class TriggerEventArgs : EventArgs {
		private readonly string triggerName;
		private readonly TableName source;
		private readonly TriggerEventType eventType;
		private readonly int fireCount;

		private readonly int oldRowIndex;
		private readonly DataRow newDataRow;

		private readonly TriggerType triggerType;

		internal TriggerEventArgs(TableName source, TriggerEventType eventType, int fireCount) 
			: this(null, source, eventType, fireCount) {
		}

		internal TriggerEventArgs(string triggerName, TableName source, TriggerEventType eventType, int fireCount) {
			this.triggerName = triggerName;
			this.source = source;
			this.eventType = eventType;
			this.fireCount = fireCount;
			triggerType = TriggerType.Callback;
		}

		internal TriggerEventArgs(TableName source, TriggerEventType eventType, int oldRowIndex, DataRow newDataRow) 
			: this(null, source, eventType, oldRowIndex, newDataRow) {
		}

		internal TriggerEventArgs(string triggerName, TableName source, TriggerEventType eventType, int oldRowIndex, DataRow newDataRow) {
			this.triggerName = triggerName;
			this.source = source;
			this.eventType = eventType;
			this.oldRowIndex = oldRowIndex;
			this.newDataRow = newDataRow;
			triggerType = TriggerType.Procedure;
		}

		public DataRow NewDataRow {
			get { return newDataRow; }
		}

		public int OldRowIndex {
			get { return oldRowIndex; }
		}

		public TriggerType TriggerType {
			get { return triggerType; }
		}

		public int FireCount {
			get { return fireCount; }
		}

		public TriggerEventType EventType {
			get { return eventType; }
		}

		public TableName Source {
			get { return source; }
		}

		public string TriggerName {
			get { return triggerName; }
		}

		public bool IsInsert {
			get { return (eventType & TriggerEventType.Insert) != 0; }
		}

		public bool IsUpdate {
			get { return (eventType & TriggerEventType.Update) != 0; }
		}

		public bool IsDelete {
			get { return (eventType & TriggerEventType.Delete) != 0; }
		}

		public bool IsBefore {
			get { return (eventType & TriggerEventType.Before) != 0; }
		}

		public bool IsAfter {
			get { return (eventType & TriggerEventType.After) != 0; }
		}

		///<summary>
		/// Verifies if the given event type matches <see cref="EventType"/>.
		///</summary>
		///<param name="toMatch"></param>
		/// <remarks>
		/// For example, if this is a BEFORE event then the BEFORE bit on the 
		/// given type must be set and if this is an INSERT event then the INSERT 
		/// bit on the given type must be set.
		/// </remarks>
		///<returns>
		/// Returns true if the given listener type should be notified of this type
		/// of table modification event.
		/// </returns>
		internal bool MatchesEventType(TriggerEventType toMatch) {
			// If this is a BEFORE trigger, then we must be listening for BEFORE events,
			// etc.
			bool baMatch =
			   ((eventType & TriggerEventType.Before) != 0 && (toMatch & TriggerEventType.Before) != 0) ||
			   ((eventType & TriggerEventType.After) != 0 && (toMatch & TriggerEventType.After) != 0);
			// If this is an INSERT trigger, then we must be listening for INSERT
			// events, etc.
			bool trigMatch =
			   ((eventType & TriggerEventType.Insert) != 0 && (toMatch & TriggerEventType.Insert) != 0) ||
			   ((eventType & TriggerEventType.Delete) != 0 && (toMatch & TriggerEventType.Delete) != 0) ||
			   ((eventType & TriggerEventType.Update) != 0 && (toMatch & TriggerEventType.Update) != 0);

			// If both of the above are true
			return (baMatch && trigMatch);
		}
	}
}