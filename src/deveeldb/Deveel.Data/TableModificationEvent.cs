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

namespace Deveel.Data {
	/// <summary>
	/// The event information of when a table is modified inside a transaction.
	/// </summary>
	class TableModificationEvent {
		/// <summary>
		/// The name of the table that was modified.
		/// </summary>
		private readonly TableName tableName;

		/// <summary>
		/// The type of event that occurred.
		/// </summary>
		private readonly TriggerEventType eventType;

		/// <summary>
		/// A DataRow object representing the row that is being inserted by 
		/// this modification.
		/// </summary>
		/// <remarks>
		/// This is set for INSERT and UPDATE events.  If the event type is 
		/// BEFORE then this data represents the new data in the table and
		/// can be modified.  This represents the NEW information.
		/// </remarks>
		private readonly DataRow dataRow;

		/// <summary>
		/// The row index of the table that is before removed by this modification.
		/// </summary>
		/// <remarks>
		/// This is set for UPDATE and DELETE events.  This represents the OLD information.
		/// </remarks>
		private readonly int rowIndex = -1;

		private TableModificationEvent(TableName tableName, int rowIndex, DataRow dataRow, TriggerEventType eventType) {
			this.tableName = tableName;
			this.rowIndex = rowIndex;
			this.dataRow = dataRow;
			this.eventType = eventType;
		}

		internal TableModificationEvent(TableName tableName, DataRow dataRow, bool before)
			: this(tableName, -1, dataRow, TriggerEventType.Insert | (before ? TriggerEventType.Before : TriggerEventType.After)) {
		}

		internal TableModificationEvent(TableName tableName, int rowIndex, DataRow dataRow, bool before)
			: this(tableName, rowIndex, dataRow, TriggerEventType.Update | (before ? TriggerEventType.Before : TriggerEventType.After)) {
		}

		internal TableModificationEvent(TableName tableName, int rowIndex, bool before)
			: this(tableName, rowIndex, null, TriggerEventType.Delete | (before ? TriggerEventType.Before : TriggerEventType.After)) {
		}

		/// <summary>
		/// Returns the event type.
		/// </summary>
		public TriggerEventType Type {
			get { return eventType; }
		}

		/// <summary>
		/// Returns true if this is a BEFORE event.
		/// </summary>
		public bool IsBefore {
			get { return (eventType & TriggerEventType.Before) != 0; }
		}

		/// <summary>
		/// Returns true if this is a AFTER event.
		/// </summary>
		public bool IsAfter {
			get { return (eventType & TriggerEventType.After) != 0; }
		}

		/// <summary>
		/// Returns the name of the table of this modification.
		/// </summary>
		public TableName TableName {
			get { return tableName; }
		}

		/// <summary>
		/// Returns the index of the row in the table that was affected by this
		/// event or -1 if event type is INSERT.
		/// </summary>
		public int RowIndex {
			get { return rowIndex; }
		}

		/// <summary>
		/// Returns the DataRow object that represents the change that is being
		/// made to the table either by an INSERT or UPDATE.  For a DELETE event this
		/// return null.
		/// </summary>
		public DataRow DataRow {
			get { return dataRow; }
		}

		///<summary>
		/// Verifies if the given listener type should be notified of this type
		/// of table modification event.
		///</summary>
		///<param name="listen_t"></param>
		/// <remarks>
		/// For example, if this is a BEFORE event then the BEFORE bit on the 
		/// given type must be set and if this is an INSERT event then the INSERT 
		/// bit on the given type must be set.
		/// </remarks>
		///<returns>
		/// Returns true if the given listener type should be notified of this type
		/// of table modification event.
		/// </returns>
		public bool IsListenedBy(TriggerEventType listen_t) {
			// If this is a BEFORE trigger, then we must be listening for BEFORE events,
			// etc.
			bool baMatch =
			   ((eventType & TriggerEventType.Before) != 0 && (listen_t & TriggerEventType.Before) != 0) ||
			   ((eventType & TriggerEventType.After) != 0 && (listen_t & TriggerEventType.After) != 0);
			// If this is an INSERT trigger, then we must be listening for INSERT
			// events, etc.
			bool trigMatch =
			   ((eventType & TriggerEventType.Insert) != 0 && (listen_t & TriggerEventType.Insert) != 0) ||
			   ((eventType & TriggerEventType.Delete) != 0 && (listen_t & TriggerEventType.Delete) != 0) ||
			   ((eventType & TriggerEventType.Update) != 0 && (listen_t & TriggerEventType.Update) != 0);
			// If both of the above are true
			return (baMatch && trigMatch);
		}
	}
}