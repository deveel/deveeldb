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

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// A new table was created during a transaction.
	/// </summary>
	public sealed class TableCreatedEvent : ObjectCreatedEvent, ITableEvent {
		/// <summary>
		/// Constructs the event object with the given table name and unique
		/// identification number.
		/// </summary>
		/// <param name="tableId">The unique identification number of the table created.</param>
		/// <param name="tableName">The unique name of the table created.</param>
		public TableCreatedEvent(int tableId, ObjectName tableName)
			: base(tableName, DbObjectType.Table) {
			TableId = tableId;
		}

		public int TableId { get; private set; }
	}
}
