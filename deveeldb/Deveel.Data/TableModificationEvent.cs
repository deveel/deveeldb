//  
//  TableModificationEvent.cs
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

namespace Deveel.Data {
	/// <summary>
	/// The event information of when a table is modified inside a transaction.
	/// </summary>
	public class TableModificationEvent {
		/// <summary>
		/// The DatabaseConnection of the table that the modification occurred in.
		/// </summary>
		private readonly DatabaseConnection connection;

		/// <summary>
		/// The name of the table that was modified.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// The type of event that occurred.
		/// </summary>
		private readonly TriggerEventType event_type;

		/// <summary>
		/// A RowData object representing the row that is being inserted by 
		/// this modification.
		/// </summary>
		/// <remarks>
		/// This is set for INSERT and UPDATE events.  If the event type is 
		/// BEFORE then this data represents the new data in the table and
		/// can be modified.  This represents the NEW information.
		/// </remarks>
		private readonly RowData row_data;

		/// <summary>
		/// The row index of the table that is before removed by this modification.
		/// </summary>
		/// <remarks>
		/// This is set for UPDATE and DELETE events.  This represents the OLD information.
		/// </remarks>
		private readonly int row_index = -1;

		private TableModificationEvent(DatabaseConnection connection, TableName table_name, int row_index, 
			RowData row_data, TriggerEventType type, bool before) {
			this.connection = connection;
			this.table_name = table_name;
			this.row_index = row_index;
			this.row_data = row_data;
			this.event_type = type | (before ? TriggerEventType.Before : TriggerEventType.After);
		}

		internal TableModificationEvent(DatabaseConnection connection, TableName table_name, RowData row_data, bool before)
			: this(connection, table_name, -1, row_data, TriggerEventType.Insert, before) {
		}

		internal TableModificationEvent(DatabaseConnection connection, TableName table_name, int row_index, RowData row_data, bool before)
			: this(connection, table_name, row_index, row_data, TriggerEventType.Update, before) {
		}

		internal TableModificationEvent(DatabaseConnection connection, TableName table_name, int row_index, bool before)
			: this(connection, table_name, row_index, null, TriggerEventType.Delete, before) {
		}

		/// <summary>
		/// Returns the DatabaseConnection that this event fired in.
		/// </summary>
		public DatabaseConnection DatabaseConnection {
			get { return connection; }
		}

		/// <summary>
		/// Returns the event type.
		/// </summary>
		public TriggerEventType Type {
			get { return event_type; }
		}

		/// <summary>
		/// Returns true if this is a BEFORE event.
		/// </summary>
		public bool IsBefore {
			get { return (event_type & TriggerEventType.Before) != 0; }
		}

		/// <summary>
		/// Returns true if this is a AFTER event.
		/// </summary>
		public bool IsAfter {
			get { return (event_type & TriggerEventType.After) != 0; }
		}

		/// <summary>
		/// Returns true if this is an INSERT event.
		/// </summary>
		public bool IsInsert {
			get { return (event_type & TriggerEventType.Insert) != 0; }
		}

		/// <summary>
		/// Returns true if this is an UPDATE event.
		/// </summary>
		public bool IsUpdate {
			get { return (event_type & TriggerEventType.Update) != 0; }
		}

		/// <summary>
		/// Returns true if this is an DELETE event.
		/// </summary>
		public bool IsDelete {
			get { return (event_type & TriggerEventType.Delete) != 0; }
		}

		/// <summary>
		/// Returns the name of the table of this modification.
		/// </summary>
		public TableName TableName {
			get { return table_name; }
		}

		/// <summary>
		/// Returns the index of the row in the table that was affected by this
		/// event or -1 if event type is INSERT.
		/// </summary>
		public int RowIndex {
			get { return row_index; }
		}

		/// <summary>
		/// Returns the RowData object that represents the change that is being
		/// made to the table either by an INSERT or UPDATE.  For a DELETE event this
		/// return null.
		/// </summary>
		public RowData RowData {
			get { return row_data; }
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
			bool ba_match =
			   ((event_type & TriggerEventType.Before) != 0 && (listen_t & TriggerEventType.Before) != 0) ||
			   ((event_type & TriggerEventType.After) != 0 && (listen_t & TriggerEventType.After) != 0);
			// If this is an INSERT trigger, then we must be listening for INSERT
			// events, etc.
			bool trig_match =
			   ((event_type & TriggerEventType.Insert) != 0 && (listen_t & TriggerEventType.Insert) != 0) ||
			   ((event_type & TriggerEventType.Delete) != 0 && (listen_t & TriggerEventType.Delete) != 0) ||
			   ((event_type & TriggerEventType.Update) != 0 && (listen_t & TriggerEventType.Update) != 0);
			// If both of the above are true
			return (ba_match && trig_match);
		}
	}
}