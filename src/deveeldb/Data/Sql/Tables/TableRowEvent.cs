﻿// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Events;

namespace Deveel.Data.Sql.Tables {
	public sealed class TableRowEvent : TableEventBase {
		public TableRowEvent(IEventSource source, ObjectName tableName, int tableId, long rowNumber, TableRowEventType eventType) 
			: base(source, tableName, tableId) {
			RowNumber = rowNumber;
			EventType = eventType;
		}

		public long RowNumber { get; }

		public TableRowEventType EventType { get; }
	}
}