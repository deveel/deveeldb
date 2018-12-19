using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	class TableEventRegistry : ITableEventRegistry {
		private readonly List<ITableEvent> events;

		public TableEventRegistry(ITableSource source, long commitId) : this(source, commitId, null) {
		}

		public TableEventRegistry(ITableSource source, long commitId, IEnumerable<ITableEvent> events) {
			TableSource = source;
			CommitId = commitId;

			this.events = events != null ? new List<ITableEvent>(events) : new List<ITableEvent>();
		}

		public IEnumerator<ITableEvent> GetEnumerator() {
			lock (this) {
				return events.GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public ITableSource TableSource { get; }

		private ObjectName TableName => TableSource.TableInfo.TableName;

		public int TableId => TableSource.TableId;

		public long CommitId { get; }

		public int EventCount {
			get {
				lock (this) {
					return events.Count;
				}
			}
		}

		public void Rollback(int count) {
			lock (this) {
				if (count > events.Count)
					throw new Exception("Trying to rollback more events than are in the registry.");

				var toAdd = new List<long>();

				// Find all entries and added new rows to the table
				foreach (var tableEvent in events.OfType<TableRowEvent>()) {
					if (tableEvent.EventType == TableRowEventType.Add ||
					    tableEvent.EventType == TableRowEventType.UpdateAdd)
						toAdd.Add(tableEvent.RowNumber);
				}

				events.RemoveRange(0, count);

				// Mark all added entries to deleted.
				foreach (var row in toAdd) {
					events.Add(new TableRowEvent(null, TableName, TableId, row, TableRowEventType.Add));
					events.Add(new TableRowEvent(null, TableName, TableId, row, TableRowEventType.Remove));
				}

			}
		}
	}
}