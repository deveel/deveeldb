// 
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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Transactions {
	public static class TableEventRegistryExtensions {
		public static IEnumerable<long> GetAddedRows(this ITableEventRegistry registry) {
			lock (registry) {
				var list = new List<long>();

				foreach (var tableEvent in registry.OfType<TableRowEvent>()) {
					var eventType = tableEvent.EventType;
					if (eventType == TableRowEventType.Add ||
					    eventType == TableRowEventType.UpdateAdd) {
						list.Add(tableEvent.RowNumber);
					} else if (eventType == TableRowEventType.Remove ||
					           eventType == TableRowEventType.UpdateRemove) {
						var index = list.IndexOf(tableEvent.RowNumber);
						if (index != -1)
							list.RemoveAt(index);
					}
				}

				return list.ToArray();
			}
		}

		public static IEnumerable<long> GetRemovedRows(this ITableEventRegistry registry) {
			lock (registry) {
				var list = new List<long>();

				foreach (var tableEvent in registry.OfType<TableRowEvent>()) {
					if (tableEvent.EventType == TableRowEventType.Remove ||
					    tableEvent.EventType == TableRowEventType.UpdateRemove)
						list.Add(tableEvent.RowNumber);
				}

				return list.ToArray();

			}
		}

		public static bool TestCommitClash(this ITableEventRegistry registry, out RowRemoveConflict conflict) {
			lock (registry) {
				// Very nasty search here...
				foreach (var rowEvent in registry.OfType<TableRowEvent>()) {
					if (rowEvent.EventType == TableRowEventType.Remove) {
						var rowNum = rowEvent.RowNumber;
						foreach (var otherRowEvent in registry.OfType<TableRowEvent>()) {
							if (otherRowEvent.RowNumber == rowNum &&
							    otherRowEvent.EventType == TableRowEventType.Remove) {
								conflict = new RowRemoveConflict(registry.TableId, rowNum);

								return true;
							}
						}
					}
				}

				conflict = new RowRemoveConflict();
				return false;
			}
		}
	}
}