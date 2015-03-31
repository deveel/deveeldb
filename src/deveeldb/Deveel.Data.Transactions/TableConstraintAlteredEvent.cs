using System;

using Deveel.Data.Deveel.Data.Transactions;

namespace Deveel.Data.Transactions {
	public class TableConstraintAlteredEvent : ITableEvent {
		public TableConstraintAlteredEvent(int tableId) {
			TableId = tableId;
		}

		public int TableId { get; private set; }
	}
}
