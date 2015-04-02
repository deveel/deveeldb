using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public class TableDroppedEvent : ObjectDroppedEvent, ITableEvent {
		public TableDroppedEvent(int tableId, ObjectName tableName)
			: base(DbObjectType.Table, tableName) {
			TableId = tableId;
		}

		public int TableId { get; private set; }
	}
}
