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
	/// A table was accessed during the transaction.
	/// </summary>
	public class TableAccessEvent : ITableEvent {
		/// <summary>
		/// Constructs the event object for the table identified
		/// by the unique number given and its unique name.
		/// </summary>
		/// <param name="tableId">The table unique identifier number.</param>
		/// <param name="tableName">The unique name of the table accessed.</param>
		public TableAccessEvent(int tableId, ObjectName tableName) {
			TableId = tableId;
			TableName = tableName;
		}

		public int TableId { get; private set; }

		/// <summary>
		/// Gets the database table unique name.
		/// </summary>
		public ObjectName TableName { get; private set; }
	}
}
