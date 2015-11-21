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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class TableEventRegistry : IEnumerable<ITableEvent> {
		private readonly List<ITableEvent> events;

		internal TableEventRegistry(TableSource tableSource) {
			if (tableSource == null)
				throw new ArgumentNullException("tableSource");

			TableSource = tableSource;
			CommitId = -1;

			events = new List<ITableEvent>();
		}

		internal TableSource TableSource { get; private set; }

		public int TableId {
			get { return TableSource.TableId; }
		}

		public long CommitId { get; internal set; }

		public IEnumerable<int> AddedRows {
			get {
				lock (this) {
					var list = new List<int>();

					foreach (var tableEvent in events.OfType<TableRowEvent>()) {
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
		}

		public IEnumerable<int> RemovedRows {
			get {
				lock (this) {
					var list = new List<int>();

					foreach (var tableEvent in events.OfType<TableRowEvent>()) {
						if (tableEvent.EventType == TableRowEventType.Remove ||
							tableEvent.EventType == TableRowEventType.UpdateRemove)
							list.Add(tableEvent.RowNumber);
					}

					return list.ToArray();
				}
			}
		}

		public int EventCount {
			get {
				lock (this) {
					return events.Count;
				}
			}
		}

		internal void Rollback(int count) {
			lock (this) {
				if (count > events.Count)
					throw new Exception("Trying to rollback more events than are in the registry.");

				List<int> toAdd = new List<int>();

				// Find all entries and added new rows to the table
				foreach (var tableEvent in events.OfType<TableRowEvent>()) {
					if (tableEvent.EventType == TableRowEventType.Add ||
						tableEvent.EventType == TableRowEventType.UpdateAdd)
						toAdd.Add(tableEvent.RowNumber);
				}

				events.RemoveRange(0, count);

				// Mark all added entries to deleted.
				for (int i = 0; i < toAdd.Count; ++i) {
					events.Add(new TableRowEvent(TableId, toAdd[i], TableRowEventType.Add));
					events.Add(new TableRowEvent(TableId, toAdd[i], TableRowEventType.Remove));
				}

			}
		}

		internal void Register(ITableEvent tableEvent) {
			lock (this) {
				events.Add(tableEvent);
			}
		}

		internal void TestCommitClash(TableInfo tableInfo, TableEventRegistry journal) {
			lock (this) {
				// Very nasty search here...
				foreach (var rowEvent in events.OfType<TableRowEvent>()) {
					if (rowEvent.EventType == TableRowEventType.Remove) {
						var rowNum = rowEvent.RowNumber;
						foreach (var otherRowEvent in journal.events.OfType<TableRowEvent>()) {
							if (otherRowEvent.RowNumber == rowNum &&
							    otherRowEvent.EventType == TableRowEventType.Remove) {
								throw new TransactionException(TransactionErrorCodes.RowRemoveClash,
									String.Format("Concurrent Serializable Transaction Conflict(1): " +
									"Current row remove clash ( row: {0}, table: {1})", rowNum, tableInfo.TableName));
							}
						}
					}
				}				
			}
		}

		public IEnumerator<ITableEvent> GetEnumerator() {
			lock (this) {
				return events.GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public ITableEvent GetEvent(int index) {
			lock (this) {
				if (index < 0 || index >= events.Count)
					throw new ArgumentOutOfRangeException("index");

				return events[index];
			}
		}
	}
}
