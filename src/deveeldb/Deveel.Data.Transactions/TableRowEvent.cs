using System;

using Deveel.Data.Deveel.Data.Transactions;

namespace Deveel.Data.Transactions {
	public class TableRowEvent : ITableEvent {
		public TableRowEvent(int tableId, int rowNumber, TableRowEventType eventType) {
			TableId = tableId;
			RowNumber = rowNumber;
			EventType = eventType;
		}

		public int TableId { get; private set; }

		public int RowNumber { get; private set; }

		public TableRowEventType EventType { get; private set; }
	}
}
