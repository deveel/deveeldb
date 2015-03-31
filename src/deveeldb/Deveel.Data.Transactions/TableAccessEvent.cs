using System;

using Deveel.Data.Deveel.Data.Transactions;

namespace Deveel.Data.Transactions {
	public class TableAccessEvent : ITableEvent {
		public TableAccessEvent(int tableId, ObjectName tableName) {
			TableId = tableId;
			TableName = tableName;
		}

		public int TableId { get; private set; }

		public ObjectName TableName { get; private set; }
	}
}
