using System;

namespace Deveel.Data.Transactions {
	public sealed class TableSelectedEvent : ITableEvent {
		public TableSelectedEvent(int tableId) {
			TableId = tableId;
		}

		public int TableId { get; private set; }
	}
}