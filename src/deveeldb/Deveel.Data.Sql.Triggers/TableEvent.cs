// 
//  Copyright 2010-2015 Deveel
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
//


using System;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// Exposes the context of an event fired on a table.
	/// </summary>
	public sealed class TableEvent : Event {
		internal TableEvent(ITable table, TriggerEventType eventType, RowId oldRowId, Row newRow) {
			if (table == null)
				throw new ArgumentNullException("table");

			Table = table;
			EventType = eventType;
			OldRowId = oldRowId;
			NewRow = newRow;
		}

		/// <summary>
		/// Gets the table on which the event occurred.
		/// </summary>
		public ITable Table { get; private set; }

		/// <summary>
		/// Gets the type of event that occurred on the table.
		/// </summary>
		public TriggerEventType EventType { get; private set; }

		/// <summary>
		/// Gets an optional reference to a row removed or updated.
		/// </summary>
		public RowId OldRowId { get; private set; }

		/// <summary>
		/// Gets the row object being added or updated.
		/// </summary>
		public Row NewRow { get; private set; }
	}
}
